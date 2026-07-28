// Package kms implements a dummy KMS server for Talos's disk-encryption KMS
// provider: the same Seal/Unseal gRPC service Talos machines call — via
// github.com/siderolabs/kms-client, the reference client library Talos
// itself uses — when a machine config selects the KMS disk-encryption
// provider (see https://www.talos.dev/latest/talos-guides/configuration/disk-encryption/#kms).
//
// It is not a real key management service: each node's key is generated
// in memory on first use and held only for the life of the process, so
// restarting Server loses every key it issued and makes every
// previously-sealed passphrase unrecoverable. It exists so local
// development and integration tests can exercise the KMS round trip
// without standing up a production-grade KMS backend.
package kms
