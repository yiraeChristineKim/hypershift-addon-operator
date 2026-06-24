# OCPBUGS-89352: Fix canary test to use `hypershift` binary instead of removed `hypershift-no-cgo`

## Problem

The upstream `openshift/hypershift` repo removed the `hypershift-no-cgo` binary from its
operator container image (PR #8601) to fix EC FIPS compliance violations. This repo's canary
test (`test/canary/run_canary_test.sh`) has an `installHypershiftBinary()` function that
extracts `/usr/bin/hypershift-no-cgo` from the hypershift operator pod. Since that binary no
longer exists, the canary test will fail.

## Fix

Update `installHypershiftBinary()` in `test/canary/run_canary_test.sh`:

1. Change the `oc rsync` source from `/usr/bin/hypershift-no-cgo` to `/usr/bin/hypershift`
2. Remove the now-unnecessary `mv /tmp/hypershift-no-cgo /tmp/hypershift` step
3. Update the comment explaining the extraction

**File:** `test/canary/run_canary_test.sh`, lines 654-665

### Before
```bash
# Extract the hypershift CLI from the hypershift operator pod. hypershift-no-cgo is built with no CGO enabled. 
${OC_COMMAND} rsync ${HO_POD_NAME}:/usr/bin/hypershift-no-cgo /tmp
...
mv /tmp/hypershift-no-cgo /tmp/hypershift
```

### After
```bash
# Extract the hypershift CLI from the hypershift operator pod
${OC_COMMAND} rsync ${HO_POD_NAME}:/usr/bin/hypershift /tmp
```

No other files in this repo reference `hypershift-no-cgo` besides this script.

## Verification

- `make fmt` and `make vet` (shell script, so no Go compilation changes)
- Visual inspection of the diff
