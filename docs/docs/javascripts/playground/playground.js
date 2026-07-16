function isDarkModeOn() {
    // Can't just use the dark mode inputs checked attribute, as it doesn't show until the user clicks on the toggle.
    // Before that, both inputs are unchecked, so we need to check the hidden attribute of the associated labels instead.
    // The labels contain the icons for each mode, so basically looking which icon is showing.
    const darkToggle = document.getElementById('__palette_1');
    const lightToggle = document.getElementById('__palette_0');
    if (darkToggle && darkToggle.labels[0].hidden) return true;
    else if (lightToggle && lightToggle.labels[0].hidden) return false;
}

// This script is loaded site-wide (mkdocs.yml extra_javascript), but the
// elements it drives only exist on the playground page, so bail out on
// every other page.
if (document.getElementById('playground-sql-input')) {
    const playgroundButton = document.getElementById('submit-sql');
    const playgroundPipelineButton = document.getElementById(
        'submit-pipeline'
    );

    const editor = ace.edit('playground-sql-input');
    //editor.setTheme("ace/theme/sqlserver");
    editor.session.setMode('ace/mode/sql');
    editor.renderer.setShowGutter(false);
    editor.setShowPrintMargin(true);
    editor.focus();
    editor.navigateFileEnd();

    const pipelineEditor = ace.edit('playground-pipeline-input');
    pipelineEditor.session.setMode('ace/mode/json');
    pipelineEditor.renderer.setShowGutter(false);
    pipelineEditor.setShowPrintMargin(true);

    const setEditorTheme = function () {
        if (isDarkModeOn()) {
            console.log('setting to monokai');
            editor.setTheme('ace/theme/monokai');
            pipelineEditor.setTheme('ace/theme/monokai');
        } else {
            console.log('setting to sqlserver light');
            editor.setTheme('ace/theme/sqlserver');
            pipelineEditor.setTheme('ace/theme/sqlserver');
        }
    };

    setEditorTheme();

    // __palette_1 and __palette_0 are ids of the dark and light mode toggle
    // radio inputs. They may not exist depending on the theme configuration,
    // so guard before attaching.
    const darkModeToggle = document.getElementById('__palette_1');
    const lightModeToggle = document.getElementById('__palette_0');
    if (darkModeToggle)
        darkModeToggle.addEventListener('change', setEditorTheme);
    if (lightModeToggle)
        lightModeToggle.addEventListener('change', setEditorTheme);

    playgroundButton.onclick = function () {
        console.log(`Running NoQL version V${SqlToMongo.VERSION}`);
        let noqlOutput = '';
        let shellQuery = '';
        let nodeQuery = '';
        let errorMessage = '';

        let optimizedPipeline = null;

        document.getElementById('playground-error-container').style.display =
            'none';
        document.getElementById(
            'playground-output-container'
        ).style.display = 'none';

        let dbDialect = 'postgresql';
        const inputSql = editor.session.getValue();
        try {
            const unwindJoins = !!document.getElementById('unwind-joins')
                .checked;
            const optimizeJoins = !!document.getElementById('optimize-joins')
                .checked;

            if (document.getElementById('force-aggregate').checked) {
                noqlOutput = SqlToMongo.makeMongoAggregate(inputSql, {
                    database: dbDialect,
                    unwindJoins: unwindJoins,
                    optimizeJoins: optimizeJoins,
                });
            } else {
                noqlOutput = SqlToMongo.parseSQL(inputSql, {
                    database: dbDialect,
                    unwindJoins: unwindJoins,
                    optimizeJoins: optimizeJoins,
                });
            }

            if (noqlOutput.type === 'aggregate' && noqlOutput.pipeline) {
                optimizedPipeline = SqlToMongo.optimizeMongoAggregate(
                    noqlOutput.pipeline,
                    {}
                );
            }

            shellQuery = constructShellQuery(noqlOutput);
            nodeQuery = constructNodeQuery(noqlOutput);

            document.getElementById('playground-noql-result').textContent =
                JSON.stringify(noqlOutput, null, 4);
            document.getElementById('playground-mongo-result').textContent =
                shellQuery;
            document.getElementById('playground-node-result').textContent =
                nodeQuery;
            document.getElementById(
                'playground-noql-optimized-result'
            ).textContent = optimizedPipeline
                ? JSON.stringify(optimizedPipeline, null, 4)
                : '';

            document.getElementById(
                'playground-output-container'
            ).style.display = 'block';
            document
                .getElementById('playground-output-container')
                .scrollIntoView({
                    block: 'center',
                    inline: 'center',
                    behavior: 'smooth',
                });
        } catch (e) {
            console.log(e);
            errorMessage = e.message;
            document.getElementById('playground-error-result').innerHTML =
                errorMessage;
            document.getElementById(
                'playground-error-container'
            ).style.display = 'block';
        }
    };

    playgroundPipelineButton.onclick = function () {
        console.log(`Running NoQL version V${SqlToMongo.VERSION}`);

        document.getElementById(
            'playground-pipeline-error-container'
        ).style.display = 'none';
        document.getElementById(
            'playground-pipeline-output-container'
        ).style.display = 'none';

        try {
            const inputPipeline = pipelineEditor.session.getValue();
            let collections = document
                .getElementById('playground-pipeline-collections')
                .value.split(',')
                .map((collection) => collection.trim())
                .filter((collection) => !!collection);

            let pipeline;
            try {
                pipeline = JSON.parse(inputPipeline);
            } catch (e) {
                throw new Error(`Invalid pipeline JSON: ${e.message}`);
            }

            // Accept a full Mongo aggregate command document
            // ({aggregate, pipeline, ...}) as well as a bare pipeline array.
            if (
                pipeline &&
                typeof pipeline === 'object' &&
                !Array.isArray(pipeline) &&
                Array.isArray(pipeline.pipeline)
            ) {
                if (
                    collections.length === 0 &&
                    typeof pipeline.aggregate === 'string' &&
                    pipeline.aggregate
                ) {
                    collections = [pipeline.aggregate];
                    document.getElementById(
                        'playground-pipeline-collections'
                    ).value = pipeline.aggregate;
                }
                pipeline = pipeline.pipeline;
            }

            const sqlOutput = SqlToMongo.pipelineToSQL(pipeline, collections);

            document.getElementById(
                'playground-pipeline-sql-result'
            ).textContent = sqlOutput;

            document.getElementById(
                'playground-pipeline-output-container'
            ).style.display = 'block';
            document
                .getElementById('playground-pipeline-output-container')
                .scrollIntoView({
                    block: 'center',
                    inline: 'center',
                    behavior: 'smooth',
                });
        } catch (e) {
            console.log(e);
            document.getElementById(
                'playground-pipeline-error-result'
            ).innerHTML = e.message;
            document.getElementById(
                'playground-pipeline-error-container'
            ).style.display = 'block';
        }
    };
}
