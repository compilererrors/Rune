# Privacy Policy

Policy version: 1.0

Effective date: April 30, 2026

Rune is a local macOS Kubernetes client. Rune does not operate a backend service for collecting, analyzing, selling, or sharing user data.

## Data Collection

Rune does not collect personal data, usage data, analytics, telemetry, advertising identifiers, tracking data, or crash analytics.

Rune does not sell data, share data with data brokers, or use data for advertising or tracking.

## Kubernetes Data

Rune communicates only with Kubernetes clusters and services that you choose to connect to. Network traffic is limited to the requests and responses required to show resources, logs, events, YAML, configuration, and related Kubernetes information in the app.

Rune does not send your cluster data, resource data, logs, manifests, namespaces, object names, or terminal output to any Rune server.

## Kubeconfig, Credentials, and Cluster Endpoints

Rune may read kubeconfig files that you select or that are available through your local Kubernetes configuration. A kubeconfig can contain cluster endpoints, context names, namespace names, certificate authority data, client certificate data, token references, exec authentication settings, and other connection information.

Rune uses this information locally on your Mac to connect to the selected Kubernetes cluster. Rune does not upload kubeconfig files, cluster endpoints, credentials, tokens, certificates, or authentication material to any Rune server.

When Rune needs to retain local connection-related secrets, it uses the macOS Keychain where appropriate. Keychain data remains on your device and is managed by macOS.

When Rune needs continued access to user-selected local files, such as kubeconfig files, it may store macOS security-scoped bookmark data locally. These bookmarks are used only to reopen files you selected and are not sent to Rune or any third party.

## Data Stored Locally

Rune stores some data locally on your Mac to provide the app's functionality. This may include app preferences, window or layout preferences, selected Kubernetes contexts and namespaces, cached Kubernetes responses, kubeconfig file references, security-scoped bookmarks for user-selected files, and connection-related secrets stored in the macOS Keychain.

This data remains on your device. It is not transmitted to Rune, collected by Rune, sold, shared, used for analytics, or used for tracking.

If you choose to export files, save diagnostics, or create a support bundle, that happens only at your direction. You control whether and how those files are shared.

## Third Parties

Rune does not include third-party analytics, advertising, tracking, or telemetry SDKs.

Rune may communicate with third-party or self-hosted Kubernetes services only when you configure those services as part of your own cluster environment.

## Changes

If this policy changes, we will update the policy version and effective date above. Policy changes are tracked in the project's git history and release history.
