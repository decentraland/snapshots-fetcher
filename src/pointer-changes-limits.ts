import { PointerChangesSyncDeployment } from '@dcl/schemas'

// These are local safety limits, deliberately much larger than real deployments. The schema bounds
// none of them, while every value is parsed from an untrusted paginated response and boundary rows are
// subsequently sorted and hashed.
const MAX_POINTERS_PER_DEPLOYMENT = 10_000
const MAX_AUTH_LINKS_PER_DEPLOYMENT = 100
const MAX_BOUNDARY_STRING_SIZE_IN_BYTES = 256 * 1024
const MAX_BOUNDARY_ROW_SIZE_IN_BYTES = 1024 * 1024

/**
 * Refuses schema-valid pointer-change rows whose structure is too large to fingerprint safely.
 *
 * @throws When a count, individual string, or the aggregate fingerprint material exceeds its limit.
 * @internal
 */
export function assertPointerChangesDeploymentWithinStructuralLimits(deployment: PointerChangesSyncDeployment): void {
  if (deployment.pointers.length > MAX_POINTERS_PER_DEPLOYMENT) {
    throw new Error(
      `Pointer-change deployment has ${deployment.pointers.length} pointers, above the maximum of ${MAX_POINTERS_PER_DEPLOYMENT}`
    )
  }
  if (deployment.authChain.length > MAX_AUTH_LINKS_PER_DEPLOYMENT) {
    throw new Error(
      `Pointer-change deployment has ${deployment.authChain.length} auth-chain links, above the maximum of ${MAX_AUTH_LINKS_PER_DEPLOYMENT}`
    )
  }

  let totalBytes = 0
  const includeString = (name: string, value: string): void => {
    const bytes = Buffer.byteLength(value, 'utf8')
    if (bytes > MAX_BOUNDARY_STRING_SIZE_IN_BYTES) {
      throw new Error(
        `Pointer-change deployment ${name} is ${bytes} bytes, above the maximum of ${MAX_BOUNDARY_STRING_SIZE_IN_BYTES}`
      )
    }
    totalBytes += bytes
    if (totalBytes > MAX_BOUNDARY_ROW_SIZE_IN_BYTES) {
      throw new Error(
        `Pointer-change deployment fingerprint fields exceed the maximum total of ${MAX_BOUNDARY_ROW_SIZE_IN_BYTES} bytes`
      )
    }
  }

  includeString('entityType', deployment.entityType)
  includeString('entityId', deployment.entityId)
  for (const pointer of deployment.pointers) {
    includeString('pointer', pointer)
  }
  for (const link of deployment.authChain) {
    includeString('auth-chain type', link.type)
    includeString('auth-chain payload', link.payload)
    includeString('auth-chain signature', link.signature ?? '')
  }
}
