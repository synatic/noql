const playgroundButton = document.getElementById('submit-sql');

var editor = ace.edit("playground-sql-input");
//editor.setTheme("ace/theme/sqlserver");
editor.session.setMode("ace/mode/sql");
editor.renderer.setShowGutter(false);
editor.setShowPrintMargin(false);
setEditorTheme();
editor.focus();
editor.navigateFileEnd();

function isDarkModeOn() {
    // Can't just use the dark mode inputs checked attribute, as it doesn't show until the user clicks on the toggle. 
    // Before that, both inputs are unchecked, so we need to check the hidden attribute of the associated labels instead.
    // The labels contain the icons for each mode, so basically looking which icon is showing.
    if (document.getElementById('__palette_2').labels[0].hidden)
        return true;
    else if (document.getElementById('__palette_1').labels[0].hidden)
        return false;
}

function setEditorTheme() {
    if (isDarkModeOn()) {
        console.log("setting to monokai");
        editor.setTheme("ace/theme/monokai");
    } else {
        console.log("setting to sqlserver light");
        editor.setTheme("ace/theme/sqlserver");
    }
}

// __palette_2 and __palette_1 are ids of the dark and light mode toggle radio inputs
document.getElementById(`__palette_2`).addEventListener('change', setEditorTheme);
document.getElementById(`__palette_1`).addEventListener('change', setEditorTheme);

playgroundButton.onclick = function () {
    
    console.log(`Running NoQL version V${SqlToMongo.VERSION}`);
    let noqlOutput = '';
    let shellQuery = '';
    let nodeQuery = '';
    let errorMessage = '';

    document.getElementById('playground-error-container').style.display = 'none';
    document.getElementById('playground-output-container').style.display = 'none';

    let dbDialect = 'postgresql';
    if (document.getElementById('dialect-mysql').checked) {
        dbDialect = 'mysql';
    }
    const inputSql = editor.session.getValue();
    try {
        if (document.getElementById('force-aggregate').checked) {
            noqlOutput = SqlToMongo.makeMongoAggregate(inputSql, { database: dbDialect });
        } else {
            noqlOutput = SqlToMongo.parseSQL(inputSql, { database: dbDialect });
        }
        shellQuery = constructShellQuery(noqlOutput);
        nodeQuery = constructNodeQuery(noqlOutput);
        document.getElementById('playground-noql-result').textContent = JSON.stringify(noqlOutput, null, 4);
        document.getElementById('playground-mongo-result').textContent = shellQuery;
        document.getElementById('playground-node-result').textContent = nodeQuery;
        document.getElementById('playground-output-container').style.display = 'block';
        document.getElementById("playground-output-container").scrollIntoView({
            behavior: 'auto',
            block: 'center',
            inline: 'center'
        });

    } catch (e) {
        console.log(e);
        errorMessage = e.message;
        document.getElementById('playground-error-result').innerHTML = errorMessage;
        document.getElementById('playground-error-container').style.display = 'block';
    }
}