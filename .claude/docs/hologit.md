# Hologit Documentation

## Overview

Hologit is a Git-based tool for compositing and transforming content from multiple sources into virtual branches. This repository uses Hologit to manage Kubernetes manifests across different deployment environments.

## Core Concepts

### Holospace

- Defined in `.holo/config.toml`
- Sets the repository namespace (currently `tdad`)
- Used as holosource in mappings that need to pull content from the host repository
- **Important**: When mapping files are named after the holospace (e.g., `_tdad.toml`), they automatically reference the host repository without needing an explicit `holosource` field

### Holosources

- External repository configurations stored in `.holo/sources/`
- Define Git repositories that can be pulled into holobranches
- Example from jarvus-ops-cluster: `cluster-template`, `helm-charts/*`

### Holobranches

- Virtual branches defined in `.holo/branches/`
- Composite content from multiple sources using mappings
- Can have subdirectories for organizing output paths

### Holomappings

- TOML files that define what content to include and where
- Support file globs, root paths, and precedence ordering
- The underscore prefix (`_name.toml`) acts as a "splat" operator, spreading content directly into the containing directory rather than creating a subdirectory
- The filename (sans underscore prefix if present) serves as the default value for `holosource`

### Hololenses

- Content transformation processors defined in `.holo/branches/<branch>.lenses/`
- Apply transformations like Kustomize builds or Helm chart rendering
- Process content after mappings are applied

## Directory Structure

```
.holo/
├── config.toml                           # Holospace configuration
├── branches/
│   ├── k8s-manifests/                   # Branch configuration
│   │   ├── _tdad.toml                   # Main mapping (splats into root)
│   │   ├── fetcher-prod/
│   │   │   └── _tdad.toml               # Environment mapping
│   │   └── fetcher-test/
│   │       └── _tdad.toml               # Environment mapping
│   └── k8s-manifests.lenses/            # Branch-specific lenses
│       ├── fetcher-prod.toml            # Kustomize lens for prod
│       ├── fetcher-test.toml            # Kustomize lens for test
│       └── k8s-normalize.toml           # Normalization lens
```

## Key Patterns

### Underscore Prefix as Splat Operator

- `_tdad.toml` → Maps content directly into the current directory
- `tdad.toml` → Would create a `tdad/` subdirectory
- Critical for maintaining correct directory structure without unwanted nesting

### Branch-Specific Lens Directories

- Lenses are stored in `.holo/branches/<branch>.lenses/`
- Keeps lens configurations separate from branch mappings
- Allows multiple lenses to be applied in sequence

### Mapping Precedence

- `before = "*"` → Apply before all other mappings
- `after = "*"` → Apply after all other mappings
- Controls the order of file merging and overrides

## Examples of Default Holosource Behavior

### Default Holosource from Filename

When you have a mapping file named `_tdad.toml` without an explicit `holosource` field:

```toml
[holomapping]
files = ["kubernetes/**"]
after = "*"
```

This is equivalent to:

```toml
[holomapping]
holosource = "tdad"  # Implicit default from filename
files = ["kubernetes/**"]
after = "*"
```

The filename (without the underscore prefix) becomes the default holosource value. This is why all `_tdad.toml` files in this repository work without explicitly specifying `holosource = "tdad"`.

### Special Holosource Syntax

Beyond the default behavior, holosources can use special syntax for advanced scenarios:

- `=>branch-name`: References another holobranch (e.g., `holosource = "=>k8s-manifests"`)
- `source=>branch`: References a branch from an external source (e.g., `holosource = "cluster-template=>main"`)
- No holosource field: Uses filename as default (as shown above)

### Quick Reference: Filename Behavior

| Filename | Default Holosource | Directory Created | Description |
|----------|--------------------|-------------------|-------------|
| `_tdad.toml` | `tdad` | None (splats into current) | Maps content directly into containing directory |
| `tdad.toml` | `tdad` | `tdad/` | Creates subdirectory for mapped content |
| `_custom.toml` | `custom` | None (splats into current) | Requires `.holo/sources/custom.toml` to exist |
| `custom.toml` | `custom` | `custom/` | Creates `custom/` subdirectory |

The underscore prefix is the key differentiator: it acts as a "splat" operator to merge content directly rather than creating a named subdirectory.

## Current Implementation: k8s-manifests Branch

### Purpose

Generates fully-rendered Kubernetes manifests for different environments by processing Kustomize overlays.

### Configuration Files

#### Main Branch Mapping

File: `.holo/branches/k8s-manifests/_tdad.toml`

- Maps GitHub workflows and basic repository structure
- Uses splat operator to place files directly in root

#### Environment-Specific Mappings

Files:

- `.holo/branches/k8s-manifests/fetcher-prod/_tdad.toml`
- `.holo/branches/k8s-manifests/fetcher-test/_tdad.toml`

Each maps both base manifests and environment-specific overlays to enable Kustomize processing.

#### Lens Configurations

Directory: `.holo/branches/k8s-manifests.lenses/`

**Kustomize Lenses** (`fetcher-prod.toml`, `fetcher-test.toml`):

- Process Kustomize overlays into fully-rendered manifests
- Each environment gets its own lens configuration
- Output replaces input with rendered manifests

**Normalization Lens** (`k8s-normalize.toml`):

- Runs after all other lenses (`after = "*"`)
- Normalizes YAML formatting across all manifests
- Excludes `.github/` directory

### Why Separate Environment Mappings?

1. **Complete File Sets**: Each Kustomize build needs both base manifests and overlay files
2. **Independent Processing**: Each environment's patches and configs are applied separately
3. **Clean Output Structure**: Produces namespace-organized directories (`fetcher-prod/`, `fetcher-test/`)
4. **Isolation**: Prevents conflicts between different environment configurations

## Comparison with jarvus-ops-cluster Patterns

The jarvus-ops-cluster repository demonstrates advanced Hologit patterns:

### Infrastructure Underlay Pattern

- Uses `cluster-template` as a base layer for infrastructure components
- Applied with `before = "*"` to provide foundation
- Includes cert-manager, ingress-nginx, metrics-server, etc.

### Application-Specific Configurations

- Each application has its own mapping configuration
- Helm charts use helm3 lens for rendering
- Kustomize apps use kustomize lens for building

### CI/CD Integration

- GitHub Actions workflow triggers holobranch projection
- Projected branch creates PR with `kubectl diff` preview
- Merge to deploy branch triggers actual deployment

## Practical Commands

### Manual Projection Testing

```bash
# Project a holobranch using working tree
git holo project k8s-manifests --working

# Project with lens execution disabled
git holo project k8s-manifests --working --no-lens

# Examine projected tree
git ls-tree -r --name-only <commit-hash>
```

### Expected Output Structure

After projection with lenses, the k8s-manifests branch contains:

```
fetcher-prod/
  ConfigMap/fetcher-config.yaml
  Deployment/consumer.yaml
  Deployment/redis.yaml
  Deployment/ticker.yaml
  Service/redis.yaml

fetcher-test/
  ConfigMap/fetcher-config.yaml
  Deployment/consumer.yaml
  Deployment/redis.yaml
  Deployment/ticker.yaml
  Service/redis.yaml
```

Each file is a fully-rendered Kubernetes manifest ready for deployment.

## Troubleshooting

### Common Issues

1. **"holosource config does not exist"**:
   - Check if the mapping file has an explicit `holosource` field
   - If not, verify the filename matches a defined holospace or existing holosource
   - Remember: `_name.toml` uses "name" as the default holosource
   - Ensure `.holo/sources/name.toml` exists if referencing an external source
2. **"holomapping config not found"**: File needs a `[holomapping]` section
3. **Files in wrong location**: Check for missing underscore prefix (splat operator)
4. **Lens not applying**: Ensure lens is in `.holo/branches/<branch>.lenses/` directory

### Debugging Tips

- Use `--working` flag to test with uncommitted changes
- Check that all referenced holosources exist in `.holo/sources/`
- Verify lens container images are accessible
- Review mapping precedence if files are being overwritten unexpectedly
