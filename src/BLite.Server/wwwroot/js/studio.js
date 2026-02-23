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
