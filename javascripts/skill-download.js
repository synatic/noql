// Download the Agent Skill zip (noql.zip) from the site assets.
(function () {
    function skillAssetUrl() {
        var base = document.querySelector('base');
        var root = base ? base.href : '/';
        return root.replace(/\/$/, '') + '/assets/noql.zip';
    }

    function triggerDownload() {
        var assetUrl = skillAssetUrl();
        fetch(assetUrl)
            .then(function (r) {
                if (!r.ok) {
                    throw new Error('Skill download failed');
                }
                return r.blob();
            })
            .then(function (blob) {
                var url = URL.createObjectURL(blob);
                var a = document.createElement('a');
                a.href = url;
                a.download = 'noql.zip';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            })
            .catch(function () {
                window.location.href = assetUrl;
            });
    }

    window.downloadSkillFile = triggerDownload;
})();
