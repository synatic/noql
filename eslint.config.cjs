const js = require('@eslint/js');
const eslintConfigPrettier = require('eslint-config-prettier');
const globals = require('globals');
const jsdoc = require('eslint-plugin-jsdoc');

module.exports = [
    {
        ignores: ['build/**', 'coverage/**', '.nyc_output/**', 'docs/**'],
    },
    js.configs.recommended,
    eslintConfigPrettier,
    {
        files: ['**/*.js'],
        languageOptions: {
            ecmaVersion: 'latest',
            sourceType: 'commonjs',
            globals: {
                ...globals.node,
            },
        },
        plugins: {
            jsdoc,
        },
        rules: {
            indent: ['error', 4, {SwitchCase: 1}],
            'max-len': 'off',
            'no-unused-vars': ['error', {args: 'none', caughtErrors: 'none'}],
            'no-prototype-builtins': 'off',
            'preserve-caught-error': 'off',
            'jsdoc/require-jsdoc': [
                'warn',
                {
                    require: {
                        FunctionDeclaration: true,
                        MethodDefinition: true,
                        ClassDeclaration: true,
                    },
                },
            ],
        },
    },
    {
        files: ['test/**/*.js'],
        languageOptions: {
            globals: {
                ...globals.node,
                ...globals.mocha,
            },
        },
    },
];
