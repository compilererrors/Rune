import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    /// Three-column workspace — shell can be tested under both native `NavigationSplitView` and AppKit-backed split behavior.
    @ViewBuilder
    var mainSplitContainer: some View {
        switch resolvedShellVariant {
        case .navigationSplitView:
            if !viewModel.isSidebarVisible || !viewModel.isDetailPaneVisible {
                compactPanelSplitContainer
            } else {
                NavigationSplitView {
                    sidebar
                        .runeAppKitFrameReporter("sidebar")
                        .background(RuneRootPaneWidthReporter(kind: .sidebar))
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                        .navigationSplitViewColumnWidth(
                            min: RuneUILayoutMetrics.splitSidebarMinWidth,
                            ideal: resolvedSidebarWidth,
                            max: RuneUILayoutMetrics.splitSidebarMaxWidth
                        )
                } content: {
                    contentPane
                        .runeAppKitFrameReporter("content")
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                        .navigationSplitViewColumnWidth(
                            min: RuneUILayoutMetrics.splitContentColumnMinWidth,
                            ideal: 760,
                            max: RuneUILayoutMetrics.splitContentColumnMaxWidth
                        )
                } detail: {
                    detailPane
                        .runeAppKitFrameReporter("detail")
                        .background(RuneRootPaneWidthReporter(kind: .detail))
                        .navigationSplitViewColumnWidth(
                            min: RuneUILayoutMetrics.splitDetailColumnMinWidth,
                            ideal: resolvedDetailWidth,
                            max: RuneUILayoutMetrics.splitDetailColumnMaxWidth
                        )
                }
                .navigationSplitViewStyle(.balanced)
            }
        case .appKitSplitView:
            AppKitTripleSplitView(
                sidebar: AnyView(
                    sidebar
                        .runeAppKitFrameReporter("sidebar")
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                ),
                content: AnyView(
                    contentPane
                        .runeAppKitFrameReporter("content")
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                ),
                detail: AnyView(
                    detailPane
                        .runeAppKitFrameReporter("detail")
                ),
                sidebarVisible: viewModel.isSidebarVisible,
                detailVisible: viewModel.isDetailPaneVisible,
                sidebarWidth: resolvedSidebarWidth,
                detailWidth: resolvedDetailWidth,
                onSidebarWidthChange: { width in
                    persistSidebarWidthIfNeeded(width)
                },
                onDetailWidthChange: { width in
                    persistDetailWidthIfNeeded(width)
                }
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }

    @ViewBuilder
    var compactPanelSplitContainer: some View {
        if !viewModel.isSidebarVisible && !viewModel.isDetailPaneVisible {
            contentPane
                .runeAppKitFrameReporter("content")
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        } else {
            HSplitView {
                if viewModel.isSidebarVisible {
                    sidebar
                        .runeAppKitFrameReporter("sidebar")
                        .background(RuneRootPaneWidthReporter(kind: .sidebar))
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                        .frame(
                            minWidth: RuneUILayoutMetrics.splitSidebarMinWidth,
                            idealWidth: resolvedSidebarWidth,
                            maxWidth: RuneUILayoutMetrics.splitSidebarMaxWidth,
                            maxHeight: .infinity,
                            alignment: .topLeading
                        )
                }

                contentPane
                    .runeAppKitFrameReporter("content")
                    .frame(
                        minWidth: RuneUILayoutMetrics.splitContentColumnMinWidth,
                        maxWidth: .infinity,
                        maxHeight: .infinity,
                        alignment: .topLeading
                    )

                if viewModel.isDetailPaneVisible {
                    detailPane
                        .runeAppKitFrameReporter("detail")
                        .background(RuneRootPaneWidthReporter(kind: .detail))
                        .frame(
                            minWidth: RuneUILayoutMetrics.splitDetailColumnMinWidth,
                            idealWidth: compactDetailIdealWidth,
                            maxWidth: compactDetailMaxWidth,
                            maxHeight: .infinity,
                            alignment: .topLeading
                        )
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }

    var background: some View {
        RuneGlassPaneSurface(role: .window)
            .ignoresSafeArea()
    }
}
