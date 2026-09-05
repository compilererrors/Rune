# Native cloud-account threat model

## Scope

This model covers local browser authorization, provider account and cluster discovery, credential refresh, native Kubernetes credential exchange, local account disconnect, diagnostics, and the handoff into Rune's existing validate → review → publish pipeline.

Native onboarding grants account access only. It must not create, change, or delete cloud accounts, scopes, IAM resources, clusters, or Kubernetes workloads. Provider-side consent revocation is separate from mandatory local secret removal.

## Protected assets

The highest-value assets are authorization codes, PKCE verifiers, access and refresh tokens, temporary role credentials, optional static credentials, and Keychain records. Account, scope, cluster, and endpoint identity are privacy-sensitive even when they are not credentials.

The active sheet, account generation, selected discovery scopes, cluster selection, connection provenance, and review result are integrity-sensitive. A stale or cross-account result must never change them.

## Trust boundaries

The system browser and its authenticated return enter Rune through a narrow callback boundary. Provider HTTPS responses enter through provider connectors. Keychain is the only persistent secret boundary. Generated kubeconfig, preferences, state restoration, diagnostics, traces, support bundles, temporary files, and the pasteboard are non-secret destinations.

Provider connectors are untrusted parsers from the coordinator's perspective. They must return opaque identities and the exact account and credential generations from their request. The coordinator independently rejects mismatches.

## Authorization interception

An attacker may observe or attempt to redeem an authorization response. Every attempt uses a fresh high-entropy PKCE verifier and S256 challenge. The verifier remains in memory and is returned only once for token exchange. Authorization codes must be short-lived and redeemed only at an allowlisted provider HTTPS endpoint with the exact redirect URI.

Native clients must never embed a confidential client secret. Bundled provider client IDs are public configuration.

## Callback injection and redirect hijacking

An attacker may invoke Rune's callback scheme, reuse a callback, change its path or origin, add duplicate parameters, or deliver a callback from a closed sheet. Rune matches the complete callback base, accepts only bounded allowlisted parameters, validates state in constant time, and consumes an attempt on every accepted callback path, including errors.

Attempts are bound to an opaque sheet ID, attempt ID, and monotonically advancing generation. Cancel, reconnect, provider switching, timeout, and sheet reopening invalidate the previous attempt. Registered callback schemes and HTTPS hosts must be explicitly allowlisted. HTTP is restricted to loopback hosts.

## Token substitution

An attacker or faulty provider response may substitute a token issued for another client, issuer, account, or authorization attempt. The code exchange carries the exact expected issuer, audience, nonce, redirect URI, provider, and generation. A provider connector must verify the signed token, issuer, audience, nonce, expiry, and provider-specific tenant or account binding before saving refresh material or publishing an account.

Rune must reject unsigned tokens, unsupported algorithms, unknown signing keys, stale keys outside a bounded refresh policy, missing expiry, excessive clock skew, and tokens whose claims do not match the attempt. The existing kubeconfig OIDC parser is not a substitute for browser-login signature verification.

## Confused-deputy and scope mixing

Results from one account, tenant, subscription, account-role pair, project, region, location, or credential generation may be incorrectly applied to another. Every request and response carries opaque account identity plus operation and credential generations. Scopes and cluster candidates carry the same provider and binding.

The coordinator rejects mismatched responses and prevents delayed discovery or refresh from overwriting a newer operation. Provider connectors must derive tokens for the exact requested provider scope and must not reuse a token or signed Kubernetes credential across a different cluster identity.

## Malicious or malformed provider responses

Provider responses may be oversized, deeply paginated, cyclic, duplicated, malformed, throttled, or partially unauthorized. Connectors must enforce the coordinator's response-byte and concurrency limits before decoding. The coordinator bounds pages, scopes, clusters, retries, retry delays, and page-token length; rejects cyclic pagination and binding mismatches; and reconciles duplicates by opaque stable identity.

Partial results remain visible with a coarse failure class and recovery action. Raw provider payloads and arbitrary provider error text must not cross into UI, logs, or diagnostics.

## Keychain and local process access

Refresh tokens, temporary credentials, and optional static secrets belong in Keychain with the strictest practical desktop-app accessibility. Account records persist only non-secret metadata and opaque references. Access tokens remain in memory and are refreshed early with one serialized refresh per binding.

A process already able to read Rune's memory or authorized Keychain items remains a platform-level risk. Release review must verify sandbox entitlements, signing identity, Keychain access groups, backup behavior, and behavior after device restore or app reinstall.

## Log and diagnostic leakage

Errors and diagnostics expose only provider, operation stage, coarse failure class, retryability, and recovery action. They omit authorization codes, provider payloads, token values, emails, account and resource identifiers, cluster names, endpoints, and query values.

Support-bundle and verbose-trace tests must scan for all seeded synthetic secrets and identifiers. New connector errors must be mapped at the connector boundary rather than forwarding localized transport or decoding errors.

## Stale asynchronous results

Browser callbacks, credential refreshes, discovery pages, imports, context reloads, and HTTP authentication failures may complete after cancellation or replacement. Cancellation alone is insufficient because a dependency may not cooperate.

All commit points therefore validate opaque operation identity, operation generation, account identity, and credential generation. A delayed authentication rejection may invalidate only the credential revision used by that request. Closing or reopening a sheet and starting a newer refresh must advance the owning generation before cancellation.

## Unsafe disconnect

Disconnect could remove the wrong account, imported kubeconfig, shared context, or external cloud resource. The disconnect request is bound to the exact local account and credential generation. Local secret deletion must be idempotent and must complete without provider availability.

Removing Rune-owned connections is a separate reviewed choice driven by explicit provenance. Imported files, watched folders, default kubeconfig, shared connections, and external provider resources are never removed by inference from a display or context name.

## Denial of service and resource retention

Repeated sign-in, pagination, throttling, and non-cooperative requests may consume memory, network, or provider quota. One active authorization attempt is retained per sheet, one refresh or discovery operation owns an account generation, and replacement cancels prior work. Provider implementations must additionally bound decoded object depth, cached metadata lifetime, network timeouts, regional fan-out, and Keychain record count.

## Release blockers

Native onboarding remains disabled for a provider until its production OAuth registration, redirect ownership, publisher or application verification, provider policy review, token-signature validation, Keychain behavior, privacy manifest, and App Store artifact are verified.

Each provider also requires deterministic connector contract tests and an external sandbox smoke test for login, discovery, review, Kubernetes authentication, expiry refresh, revocation, reconnect, and disconnect. Synthetic repository fixtures must not contain real account or infrastructure data.
