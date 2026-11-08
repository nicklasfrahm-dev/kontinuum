package ui

import (
	"bytes"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/auth"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/identityprovider"
)

const (
	// viewIdentityProviders is the IAM pages' "Identity providers" tab.
	viewIdentityProviders = "providers"
	// maxIdentityProviderFormBytes bounds the "Add identity provider"
	// form's body — larger than the role forms' own limit, since it can
	// carry a PEM CA bundle.
	maxIdentityProviderFormBytes = 1 << 17
	// defaultUsernameExpression, defaultGroupsExpression, and
	// defaultEmailVerifiedRule prefill a fresh "Add identity provider"
	// form with the mapping most OIDC issuers need. The email-verified
	// rule isn't optional decoration: kube-apiserver's own validation
	// rejects a username expression reading claims.email unless
	// claims.email_verified is checked too.
	defaultUsernameExpression = "claims.email"
	defaultGroupsExpression   = "has(claims.groups) ? claims.groups : []"
	defaultEmailVerifiedRule  = "claims.?email_verified.orValue(true) == true"
	// clusterIdentityProvidersPath is the cluster-scoped list's route.
	clusterIdentityProvidersPath = "/app/kontinuum.sh/v1alpha2/clusteridentityproviders"
)

// celRuleFieldPattern matches the "Add identity provider" form's rule
// builder field names, e.g. "claimRules[0][expression]" — see
// identity_provider_add_modal.html's own "+ Add rule" script.
var celRuleFieldPattern = regexp.MustCompile(`^(claimRules|userRules)\[(\d+)]\[(expression|message)]$`)

// identityProvidersPath returns the namespaced list's route.
func identityProvidersPath(namespace string) string {
	return "/app/kontinuum.sh/v1alpha2/namespaces/" + namespace + "/identityproviders"
}

// identityProviderRow is one IdentityProvider or ClusterIdentityProvider
// rendered on the IAM pages' "Identity providers" tab.
type identityProviderRow struct {
	Name string
	// KindLabel names the row's kind in its delete dialog.
	KindLabel       string
	DeleteURL       string
	Issuer          string
	Audiences       []string
	UsernameMapping string
	GroupsMapping   string
	Prefix          string
	SigningKeys     int32
	LastKeyRefresh  string
	Ready           bool
	Reason          string
	Message         string
	Age             string
}

// identityProviderRowFrom maps one provider of either kind onto a row.
func identityProviderRowFrom(
	objectMeta metav1.ObjectMeta, spec v1alpha2.IdentityProviderSpec, status v1alpha2.IdentityProviderStatus,
	deleteURL string,
) identityProviderRow {
	kindLabel := "cluster identity provider"
	if objectMeta.Namespace != "" {
		kindLabel = "identity provider"
	}

	row := identityProviderRow{
		Name:            objectMeta.Name,
		KindLabel:       kindLabel,
		DeleteURL:       deleteURL,
		Issuer:          spec.Issuer.URL,
		Audiences:       spec.Issuer.Audiences,
		UsernameMapping: mappingSummary(spec.ClaimMappings.Username),
		GroupsMapping:   mappingSummary(spec.ClaimMappings.Groups),
		Prefix:          identityprovider.IdentityPrefix(objectMeta.Namespace),
		SigningKeys:     status.SigningKeys,
		Reason:          "Pending",
		Message:         "Waiting for the controller to pick this identity provider up.",
		Age:             formatAge(objectMeta.CreationTimestamp.Time),
	}

	if status.LastKeyRefreshTime != nil {
		row.LastKeyRefresh = formatAge(status.LastKeyRefreshTime.Time)
	}

	ready := meta.FindStatusCondition(status.Conditions, identityprovider.ReadyConditionType)
	if ready != nil {
		row.Ready = ready.Status == metav1.ConditionTrue
		row.Reason = ready.Reason
		row.Message = ready.Message
	}

	return row
}

// mappingSummary renders one claim mapping as a single line: its CEL
// expression, or its claim and prefix.
func mappingSummary(mapping v1alpha2.PrefixedClaimOrExpression) string {
	if mapping.Expression != "" {
		return mapping.Expression
	}

	if mapping.Claim == "" {
		return ""
	}

	if mapping.Prefix != nil && *mapping.Prefix != "" {
		return fmt.Sprintf("%q + claims.%s", *mapping.Prefix, mapping.Claim)
	}

	return "claims." + mapping.Claim
}

// handleIAMNamespaceIdentityProviders is GET
// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/identityproviders's handler.
func (r *Router) handleIAMNamespaceIdentityProviders(writer http.ResponseWriter, request *http.Request) {
	r.renderIAMNamespace(writer, request, viewIdentityProviders)
}

// handleIAMClusterIdentityProviders is GET
// /app/kontinuum.sh/v1alpha2/clusteridentityproviders's handler.
func (r *Router) handleIAMClusterIdentityProviders(writer http.ResponseWriter, request *http.Request) {
	r.renderIAMCluster(writer, request, viewIdentityProviders)
}

// loadIdentityProviders lists namespace's IdentityProviders — or every
// ClusterIdentityProvider when namespace is empty — into
// data["IdentityProviders"]. ok is false only when this has already
// written the response itself, same contract as listNamespaceRoles.
func (r *Router) loadIdentityProviders(
	writer http.ResponseWriter, request *http.Request, kontinuums KontinuumClient, namespace string,
	data map[string]any,
) bool {
	rows, err := r.listIdentityProviders(request, kontinuums, namespace)
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return false
		}

		http.Error(writer, "failed to list identity providers: "+err.Error(), http.StatusBadGateway)

		return false
	}

	data["IdentityProviders"] = rows

	return true
}

// listIdentityProviders lists the providers of the right kind for
// namespace, sorted by name.
func (r *Router) listIdentityProviders(
	request *http.Request, kontinuums KontinuumClient, namespace string,
) ([]identityProviderRow, error) {
	var rows []identityProviderRow

	if namespace == "" {
		var list v1alpha2.ClusterIdentityProviderList

		err := kontinuums.List(request.Context(), &list)
		if err != nil {
			return nil, fmt.Errorf("failed to list cluster identity providers: %w", err)
		}

		for _, item := range list.Items {
			rows = append(rows, identityProviderRowFrom(item.ObjectMeta, item.Spec, item.Status,
				clusterIdentityProvidersPath+"/"+item.Name))
		}
	} else {
		var list v1alpha2.IdentityProviderList

		err := kontinuums.List(request.Context(), &list, client.InNamespace(namespace))
		if err != nil {
			return nil, fmt.Errorf("failed to list identity providers: %w", err)
		}

		for _, item := range list.Items {
			rows = append(rows, identityProviderRowFrom(item.ObjectMeta, item.Spec, item.Status,
				identityProvidersPath(namespace)+"/"+item.Name))
		}
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	return rows, nil
}

// celRuleFields is one rule builder row's raw submitted values.
type celRuleFields struct {
	Expression string
	Message    string
}

// identityProviderAddFields is the "Add identity provider" form's raw
// submitted values, preserved as-is so a rejected submission can be
// redisplayed exactly as typed.
type identityProviderAddFields struct {
	Name                 string
	IssuerURL            string
	Audiences            string
	DiscoveryURL         string
	CertificateAuthority string
	UsernameExpression   string
	GroupsExpression     string
	UIDExpression        string
	ClaimRules           []celRuleFields
	UserRules            []celRuleFields
}

// defaultIdentityProviderFields prefills a fresh form.
func defaultIdentityProviderFields() identityProviderAddFields {
	return identityProviderAddFields{
		UsernameExpression: defaultUsernameExpression,
		GroupsExpression:   defaultGroupsExpression,
		ClaimRules:         []celRuleFields{{Expression: defaultEmailVerifiedRule}},
	}
}

// identityProviderAddFormData is the "identity-provider-add-modal-body"
// fragment's template data, for namespace's IdentityProvider form or —
// when namespace is empty — the ClusterIdentityProvider form.
func identityProviderAddFormData(
	namespace string, fields identityProviderAddFields, created, formErr string,
) map[string]any {
	data := map[string]any{
		"IDPAddURL": clusterIdentityProvidersPath, "IDPKind": "cluster identity provider",
		"IDPFields": fields, "IDPPrefix": identityprovider.IdentityPrefix(namespace),
		"CreatedIDP": created, "IDPError": formErr,
	}

	if namespace != "" {
		data["IDPAddURL"] = identityProvidersPath(namespace)
		data["IDPKind"] = "identity provider"
	}

	return data
}

// renderIdentityProviderAddModalBody renders just the
// "identity-provider-add-modal-body" fragment from page's own template
// tree — see renderRoleAddModalBody, the precedent this follows.
func (r *Router) renderIdentityProviderAddModalBody(writer http.ResponseWriter, page string, data map[string]any) {
	var buf bytes.Buffer

	err := r.pages[page].ExecuteTemplate(&buf, "identity-provider-add-modal-body", data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(writer)
}

// handleIdentityProviderAdd is POST
// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/identityproviders's handler.
func (r *Router) handleIdentityProviderAdd(writer http.ResponseWriter, request *http.Request) {
	r.addIdentityProvider(writer, request, request.PathValue("ns"), pageIAMNamespace)
}

// handleClusterIdentityProviderAdd is POST
// /app/kontinuum.sh/v1alpha2/clusteridentityproviders's handler.
func (r *Router) handleClusterIdentityProviderAdd(writer http.ResponseWriter, request *http.Request) {
	r.addIdentityProvider(writer, request, "", pageIAMCluster)
}

// addIdentityProvider creates the submitted provider — an IdentityProvider
// in namespace, or a ClusterIdentityProvider when namespace is empty. The
// spec is validated (CEL included) before anything is created, so a typo
// in an expression is reported in the dialog rather than only later on the
// object's status. On success it swaps the modal body to a toast marker;
// on failure it re-renders the form with the submitted values preserved —
// see handleRoleAdd, the precedent this follows.
func (r *Router) addIdentityProvider(writer http.ResponseWriter, request *http.Request, namespace, page string) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxIdentityProviderFormBytes)

	err := request.ParseForm()
	if err != nil {
		http.Error(writer, "failed to parse form: "+err.Error(), http.StatusBadRequest)

		return
	}

	fields := parseIdentityProviderForm(request.PostForm)

	formErr := validateIdentityProviderFields(fields)
	if formErr != "" {
		r.renderIdentityProviderAddModalBody(writer, page, identityProviderAddFormData(namespace, fields, "", formErr))

		return
	}

	spec := buildIdentityProviderSpec(fields)

	err = identityprovider.ValidateSpec(spec)
	if err != nil {
		r.renderIdentityProviderAddModalBody(writer, page, identityProviderAddFormData(namespace, fields, "", err.Error()))

		return
	}

	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	objectMeta := metav1.ObjectMeta{Name: fields.Name, Namespace: namespace}

	if namespace == "" {
		err = kontinuums.Create(request.Context(), &v1alpha2.ClusterIdentityProvider{ObjectMeta: objectMeta, Spec: spec})
	} else {
		err = kontinuums.Create(request.Context(), &v1alpha2.IdentityProvider{ObjectMeta: objectMeta, Spec: spec})
	}

	if err != nil {
		r.renderIdentityProviderAddModalBody(writer, page, identityProviderAddFormData(namespace, fields, "", err.Error()))

		return
	}

	r.renderIdentityProviderAddModalBody(writer, page,
		identityProviderAddFormData(namespace, defaultIdentityProviderFields(), fields.Name, ""))
}

// parseIdentityProviderForm reads the submitted form into fields.
func parseIdentityProviderForm(form map[string][]string) identityProviderAddFields {
	value := func(key string) string {
		values := form[key]
		if len(values) == 0 {
			return ""
		}

		return strings.TrimSpace(values[0])
	}

	return identityProviderAddFields{
		Name:                 value("name"),
		IssuerURL:            value("issuerURL"),
		Audiences:            value("audiences"),
		DiscoveryURL:         value("discoveryURL"),
		CertificateAuthority: value("certificateAuthority"),
		UsernameExpression:   value("usernameExpression"),
		GroupsExpression:     value("groupsExpression"),
		UIDExpression:        value("uidExpression"),
		ClaimRules:           parseCELRules(form, "claimRules"),
		UserRules:            parseCELRules(form, "userRules"),
	}
}

// parseCELRules extracts group's rule builder rows in ascending index
// order, dropping rows left entirely blank.
func parseCELRules(form map[string][]string, group string) []celRuleFields {
	byIndex := collectCELRuleRows(form, group)

	indexes := make([]int, 0, len(byIndex))
	for index := range byIndex {
		indexes = append(indexes, index)
	}

	sort.Ints(indexes)

	rules := make([]celRuleFields, 0, len(indexes))

	for _, index := range indexes {
		row := byIndex[index]
		if row.Expression == "" && row.Message == "" {
			continue
		}

		rules = append(rules, *row)
	}

	return rules
}

// collectCELRuleRows groups group's submitted rule fields by row index.
func collectCELRuleRows(form map[string][]string, group string) map[int]*celRuleFields {
	byIndex := map[int]*celRuleFields{}

	for key, values := range form {
		match := celRuleFieldPattern.FindStringSubmatch(key)
		if match == nil || match[1] != group || len(values) == 0 {
			continue
		}

		index, err := strconv.Atoi(match[2])
		if err != nil {
			continue
		}

		row, ok := byIndex[index]
		if !ok {
			row = &celRuleFields{}
			byIndex[index] = row
		}

		if match[3] == "expression" {
			row.Expression = strings.TrimSpace(values[0])
		} else {
			row.Message = strings.TrimSpace(values[0])
		}
	}

	return byIndex
}

// validateIdentityProviderFields returns a human-readable error for a
// missing required field, or "".
func validateIdentityProviderFields(fields identityProviderAddFields) string {
	switch {
	case fields.Name == "":
		return "Name is required."
	case fields.IssuerURL == "":
		return "Issuer URL is required."
	case len(splitCommaList(fields.Audiences)) == 0:
		return "At least one audience is required."
	case fields.UsernameExpression == "":
		return "A username expression is required."
	default:
		return ""
	}
}

// buildIdentityProviderSpec converts accepted form fields into a spec.
func buildIdentityProviderSpec(fields identityProviderAddFields) v1alpha2.IdentityProviderSpec {
	audiences := splitCommaList(fields.Audiences)

	spec := v1alpha2.IdentityProviderSpec{
		Issuer: v1alpha2.IdentityProviderIssuer{
			URL:                  fields.IssuerURL,
			DiscoveryURL:         fields.DiscoveryURL,
			Audiences:            audiences,
			CertificateAuthority: fields.CertificateAuthority,
		},
		ClaimMappings: v1alpha2.ClaimMappings{
			Username: v1alpha2.PrefixedClaimOrExpression{Expression: fields.UsernameExpression},
			Groups:   v1alpha2.PrefixedClaimOrExpression{Expression: fields.GroupsExpression},
			UID:      v1alpha2.ClaimOrExpression{Expression: fields.UIDExpression},
		},
	}

	if len(audiences) > 1 {
		spec.Issuer.AudienceMatchPolicy = v1alpha2.AudienceMatchPolicyMatchAny
	}

	for _, rule := range fields.ClaimRules {
		spec.ClaimValidationRules = append(spec.ClaimValidationRules,
			v1alpha2.ClaimValidationRule{Expression: rule.Expression, Message: rule.Message})
	}

	for _, rule := range fields.UserRules {
		spec.UserValidationRules = append(spec.UserValidationRules,
			v1alpha2.UserValidationRule{Expression: rule.Expression, Message: rule.Message})
	}

	return spec
}

// handleDeleteIdentityProvider is DELETE
// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/identityproviders/{name}'s
// handler — see handleDeleteRole, the precedent this follows.
func (r *Router) handleDeleteIdentityProvider(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	namespace := request.PathValue("ns")
	target := &v1alpha2.IdentityProvider{
		ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name"), Namespace: namespace},
	}

	r.deleteAndRedirect(writer, request, kontinuums, target, "identity provider", identityProvidersPath(namespace))
}

// handleDeleteClusterIdentityProvider is DELETE
// /app/kontinuum.sh/v1alpha2/clusteridentityproviders/{name}'s handler.
func (r *Router) handleDeleteClusterIdentityProvider(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	target := &v1alpha2.ClusterIdentityProvider{ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name")}}

	r.deleteAndRedirect(writer, request, kontinuums, target, "cluster identity provider", clusterIdentityProvidersPath)
}
