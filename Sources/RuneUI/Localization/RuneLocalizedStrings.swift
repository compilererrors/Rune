import Foundation

public enum RuneLanguage: String, CaseIterable, Identifiable, Sendable {
    case english = "en"
    case spanish = "es"
    case german = "de"
    case french = "fr"
    case hindi = "hi"
    case simplifiedChinese = "zh-Hans"

    public var id: String { rawValue }

    public var displayName: String {
        switch self {
        case .english: return "English"
        case .spanish: return "Español"
        case .german: return "Deutsch"
        case .french: return "Français"
        case .hindi: return "हिन्दी"
        case .simplifiedChinese: return "简体中文"
        }
    }

    public static func resolved(_ rawValue: String) -> RuneLanguage {
        RuneLanguage(rawValue: rawValue) ?? .english
    }
}

public enum RuneLocalizedStringKey: String, CaseIterable, Sendable {
    case allContainers
    case apply
    case applyYAML
    case chooseContainerHelp
    case clusterRoles
    case clusterRoleBindings
    case commands
    case config
    case configMaps
    case copyAll
    case copyAndExportLogOutputHelp
    case copySelection
    case cronJobs
    case daemonSets
    case deployments
    case describe
    case done
    case draft
    case edit
    case endpoints
    case events
    case exec
    case exportAllPodsFullZip
    case exportDescribe
    case exportFullUnfilteredZip
    case exportVisibleResultsZip
    case file
    case findInDescribe
    case findInLogs
    case findInYAML
    case helm
    case hideManaged
    case history
    case horizontalPodAutoscalers
    case ingresses
    case jobs
    case kind
    case logs
    case manifest
    case matchCase
    case networking
    case networkPolicies
    case nodes
    case operatorResources
    case overview
    case persistentVolumeClaims
    case persistentVolumes
    case portForward
    case container
    case language
    case more
    case pause
    case pauseTailHelp
    case pod
    case pods
    case previous
    case previousLogsHelp
    case quickEdit
    case releases
    case replicaSets
    case reload
    case reloadLogsHelp
    case resume
    case resumeTailHelp
    case rbac
    case roles
    case roleBindings
    case rollout
    case saveCurrentLogsHelp
    case saveLogs
    case searchLogs
    case secrets
    case serviceAccounts
    case services
    case settingsAppearance
    case settingsCache
    case settingsClearCachedClusterData
    case settingsDemoCluster
    case settingsDiagnostics
    case settingsDiagnosticsSubtitle
    case settingsFontSize
    case settingsFontSizeDetail
    case settingsGeneral
    case settingsGeneralSubtitle
    case settingsHideManagedFieldsByDefault
    case settingsHideManagedFieldsByDefaultHelp
    case settingsKeyBindings
    case settingsKeyBindingsSubtitle
    case settingsLanguageDetail
    case settingsLogs
    case settingsLogsSubtitle
    case settingsMaintenance
    case settingsPerformance
    case settingsPerformanceSubtitle
    case settingsPersistNamespaceListCache
    case settingsPersistNamespaceListCacheHelp
    case settingsReset
    case settingsSafety
    case settingsSafetySubtitle
    case settingsShowDemoClusterContext
    case settingsShowDemoClusterContextHelp
    case settingsShowHoverTooltips
    case settingsShowHoverTooltipsHelp
    case settingsShowResourceTableScrollEdgeGlow
    case settingsShowResourceTableScrollEdgeGlowHelp
    case settingsSimpleMode
    case settingsSimpleModeHelp
    case settingsSimpleModeManagedFieldsNote
    case settingsThemes
    case settingsThemesSubtitle
    case startTailHelp
    case statefulSets
    case storage
    case storageClasses
    case stopTail
    case tail
    case terminal
    case unifiedLogs
    case unsavedEdits
    case values
    case workloads
    case window
    case yaml
    case yamlEditsStayLocal
    case yamlManifest
}

public struct RuneLocalizedStrings: Sendable {
    public static let shared = RuneLocalizedStrings()

    private let tables: [RuneLanguage: [String: String]]

    public init() {
        self.init(bundle: Self.defaultBundle())
    }

    public init(bundle: Bundle) {
        var loadedTables: [RuneLanguage: [String: String]] = [:]
        for language in RuneLanguage.allCases {
            loadedTables[language] = Self.loadTable(language: language, bundle: bundle)
        }
        tables = loadedTables
    }

    public func string(_ key: RuneLocalizedStringKey, language: RuneLanguage) -> String {
        if let localized = tables[language]?[key.rawValue], !localized.isEmpty {
            return localized
        }
        if let fallback = tables[.english]?[key.rawValue], !fallback.isEmpty {
            return fallback
        }
        return key.rawValue
    }

    private static func defaultBundle() -> Bundle {
        if let appBundle = appResourceBundle() {
            return appBundle
        }
        return .module
    }

    private static func appResourceBundle() -> Bundle? {
        let bundleNames = ["Rune_RuneUI.bundle", "RuneUI_RuneUI.bundle", "RuneUI.bundle"]
        let baseURLs = [
            Bundle.main.resourceURL,
            Bundle.main.bundleURL
        ].compactMap { $0 }

        return preferredAppResourceBundle(baseURLs: baseURLs, bundleNames: bundleNames)
    }

    static func preferredAppResourceBundle(
        baseURLs: [URL],
        bundleNames: [String] = ["Rune_RuneUI.bundle", "RuneUI_RuneUI.bundle", "RuneUI.bundle"]
    ) -> Bundle? {
        for baseURL in baseURLs {
            for bundleName in bundleNames {
                let url = baseURL.appendingPathComponent(bundleName)
                guard let bundle = Bundle(url: url), containsLocalizationCatalog(in: bundle) else {
                    continue
                }
                return bundle
            }
        }
        return nil
    }

    private static func containsLocalizationCatalog(in bundle: Bundle) -> Bool {
        bundle.url(forResource: RuneLanguage.english.rawValue, withExtension: "json", subdirectory: "RuneLocalizations") != nil
            || bundle.url(forResource: RuneLanguage.english.rawValue, withExtension: "json") != nil
    }

    private static func loadTable(language: RuneLanguage, bundle: Bundle) -> [String: String] {
        let url = bundle.url(
            forResource: language.rawValue,
            withExtension: "json",
            subdirectory: "RuneLocalizations"
        ) ?? bundle.url(
            forResource: language.rawValue,
            withExtension: "json"
        )

        guard
            let url,
            let data = try? Data(contentsOf: url),
            let table = try? JSONDecoder().decode([String: String].self, from: data)
        else {
            return [:]
        }
        return table
    }
}
