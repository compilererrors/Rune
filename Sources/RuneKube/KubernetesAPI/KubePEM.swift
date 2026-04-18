import Foundation

enum KubePEM {
    /// Returns DER bytes from a PEM block (e.g. `certificate-authority-data` after base64 decode is often already PEM text in kubeconfig — here we expect **decoded file contents** as UTF-8 PEM).
    static func derFromCertificatePEM(_ pem: Data) -> Data? {
        guard let s = String(data: pem, encoding: .utf8) else { return nil }
        return derFromPEMBlock(s, label: "CERTIFICATE")
    }

    static func derFromPEMBlock(_ pem: String, label: String) -> Data? {
        let begin = "-----BEGIN \(label)-----"
        let end = "-----END \(label)-----"
        guard let r = pem.range(of: begin), let e = pem.range(of: end, range: r.upperBound ..< pem.endIndex) else {
            return nil
        }
        let b64 = pem[r.upperBound ..< e.lowerBound]
            .replacingOccurrences(of: "\n", with: "")
            .replacingOccurrences(of: "\r", with: "")
        return Data(base64Encoded: String(b64))
    }
}
