// BLite Studio — client-side helpers
window.studioClipboard = {
    copy: async function (text) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch {
            return false;
        }
    }
};

// Triggers a browser "Save As" dialog for binary data passed as a base64 string.
window.studioDownload = {
    saveAs: function (fileName, mimeType, base64Data) {
        const bytes = Uint8Array.from(atob(base64Data), c => c.charCodeAt(0));
        const blob  = new Blob([bytes], { type: mimeType });
        const url   = URL.createObjectURL(blob);
        const a     = document.createElement('a');
        a.href     = url;
        a.download = fileName;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }
};
