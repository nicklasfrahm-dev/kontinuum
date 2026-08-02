package taloscluster

import (
	"errors"
	"fmt"

	"github.com/google/cel-go/cel"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// celFieldKey marks a position in an embedded default values file as
// computed rather than literal — see evaluateComputedValues.
const celFieldKey = "$cel"

// errComputedValuesNotMap is a static sentinel — err113 flags a
// dynamically constructed errors.New/fmt.Errorf call without a wrapped
// static error, same as controller.go's own errKubeconfigNotStored.
var errComputedValuesNotMap = errors.New("resolved computed values is not a map")

// celEnv declares this package's one CEL variable: ctx, a dynamic value
// (see celContext) — deliberately a single extensible bag rather than a
// per-fact declared variable, so a new $cel rule needing another piece of
// context doesn't require touching this function.
func celEnv() (*cel.Env, error) {
	env, err := cel.NewEnv(cel.Variable("ctx", cel.DynType))
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL environment: %w", err)
	}

	return env, nil
}

// celContext builds the ctx value every $cel expression in an embedded
// values/*.yaml evaluates against — two namespaced top-level groups
// rather than a flat bag of ad hoc names, so where a fact belongs is
// obvious from its own path:
//
//   - ctx.taloscluster is cluster's own resource (spec, status, metadata
//     — the same shape `kubectl get talosclusters.kontinuum.sh <name> -o
//     json` would print), plus one reconciler-computed addition at its
//     own natural status path: status.controlPlane.replicas — the
//     control-plane pool's current claimed-Instance count, something
//     TalosCluster's real API type doesn't persist itself (see
//     Reconciler.controlPlaneCount), placed here rather than as a
//     same-level sibling key so a future real status field of the same
//     name would need no expression changes to adopt.
//   - ctx.talos carries facts about Talos itself, not any particular
//     TalosCluster — currently just kubePrism.port, the fixed local port
//     Talos's own KubePrism apiserver proxy listens on (see config.go's
//     kubePrismPort).
//
// Any expression can dot into whatever field it needs without a Go
// change, as long as it already lives somewhere in this shape.
func celContext(cluster *v1alpha2.TalosCluster, controlPlaneCount int) (map[string]any, error) {
	unstructuredCluster, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to convert TalosCluster to CEL context: %w", err)
	}

	status, _ := unstructuredCluster["status"].(map[string]any)
	if status == nil {
		status = map[string]any{}
	}

	status["controlPlane"] = map[string]any{"replicas": controlPlaneCount}
	unstructuredCluster["status"] = status

	return map[string]any{
		"taloscluster": unstructuredCluster,
		"talos": map[string]any{
			"kubePrism": map[string]any{"port": kubePrismPort},
		},
	}, nil
}

// evaluateComputedValues walks values recursively, substituting every
// {"$cel": "<expr>"} it finds with <expr>'s evaluated result against
// celCtx (built by celContext — exposed to the expression itself as the
// declared "ctx" variable). nil-safe: a nil/empty values returns as-is,
// no CEL environment even built.
func evaluateComputedValues(values map[string]any, celCtx map[string]any) (map[string]any, error) {
	if len(values) == 0 {
		return values, nil
	}

	env, err := celEnv()
	if err != nil {
		return nil, err
	}

	resolved, err := evalNode(env, values, celCtx)
	if err != nil {
		return nil, err
	}

	resolvedMap, ok := resolved.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%T: %w", resolved, errComputedValuesNotMap)
	}

	return resolvedMap, nil
}

// evalNode resolves one node of a values tree: a map holding exactly one
// "$cel" key evaluates as an expression; any other map or slice is walked
// recursively; anything else is returned unchanged.
func evalNode(env *cel.Env, node any, celCtx map[string]any) (any, error) {
	switch typedNode := node.(type) {
	case map[string]any:
		if expr, ok := celExpr(typedNode); ok {
			return evalExpr(env, expr, celCtx)
		}

		resolved := make(map[string]any, len(typedNode))

		for key, child := range typedNode {
			resolvedChild, err := evalNode(env, child, celCtx)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve %q: %w", key, err)
			}

			resolved[key] = resolvedChild
		}

		return resolved, nil
	case []any:
		resolved := make([]any, len(typedNode))

		for index, child := range typedNode {
			resolvedChild, err := evalNode(env, child, celCtx)
			if err != nil {
				return nil, err
			}

			resolved[index] = resolvedChild
		}

		return resolved, nil
	default:
		return typedNode, nil
	}
}

// celExpr reports whether node is a "$cel" marker — a map with exactly
// one key, celFieldKey, holding a string expression.
func celExpr(node map[string]any) (string, bool) {
	if len(node) != 1 {
		return "", false
	}

	raw, hasCelKey := node[celFieldKey]
	if !hasCelKey {
		return "", false
	}

	expr, isString := raw.(string)

	return expr, isString
}

// evalExpr compiles and evaluates expr against celCtx, exposed to the
// expression itself as its declared "ctx" variable.
func evalExpr(env *cel.Env, expr string, celCtx map[string]any) (any, error) {
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return nil, fmt.Errorf("failed to compile CEL expression %q: %w", expr, iss.Err())
	}

	prg, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL program for %q: %w", expr, err)
	}

	out, _, err := prg.Eval(map[string]any{"ctx": celCtx})
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate CEL expression %q: %w", expr, err)
	}

	return out.Value(), nil
}
