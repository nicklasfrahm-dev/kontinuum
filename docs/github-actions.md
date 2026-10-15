# GitHub Actions

The [`setup-kontinuum`](https://github.com/nicklasfrahm-dev/kontinuum/tree/main/actions/setup-kontinuum) action installs the kontinuum CLI on a GitHub Actions runner and adds it to the `PATH`.

## Usage

```yaml
steps:
  - name: Set up kontinuum
    uses: nicklasfrahm-dev/kontinuum/actions/setup-kontinuum@main
    with:
      version: v0.36.0

  - name: Print kontinuum version
    run: kontinuum version
```

## Inputs

| Name           | Default                              | Description                                                                                          |
| -------------- | ------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| `version`      | `latest`                             | The exact version to install (`v0.36.0` or `0.36.0`), or `latest` for the most recent GitHub release. |
| `image`        | `ghcr.io/nicklasfrahm-dev/kontinuum` | The container image to extract the binary from. Its tags must match the release tags.                |
| `github-token` | `${{ github.token }}`                | The token used to resolve `latest` via the GitHub API.                                               |

## Outputs

| Name      | Description                                    |
| --------- | ---------------------------------------------- |
| `version` | The kontinuum version that was installed.      |
| `path`    | The absolute path to the installed binary.     |

## How it works

kontinuum does not publish standalone binaries as release assets, so the action copies the statically linked binary out of the release container image with `docker create` and `docker cp`. This means:

- The runner must be a Linux runner (`amd64` or `arm64`) with Docker available, which all GitHub-hosted Ubuntu runners are.
- `latest` resolves to the most recent GitHub release rather than the image's own `latest` tag, since that tag is pushed on every commit to `main`.
- The binary is cached in the runner's tool cache, so repeated runs on a self-hosted runner skip the download.
