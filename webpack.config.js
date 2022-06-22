const path = require('path');
const {CleanWebpackPlugin} = require('clean-webpack-plugin');
// const WebpackBundleAnalyzer = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

const config = {
    target: 'web',
    entry: {
        index: './index.js',
    },
    output: {
        path: path.resolve(__dirname, './dist'),
        filename: 'index.js',
        library: 'SqlToMongo',
        libraryTarget: 'umd',
        globalObject: 'this',
        umdNamedDefine: true,
    },
    watchOptions: {
        aggregateTimeout: 600,
        ignored: /node_modules/,
    },
    plugins: [
        new CleanWebpackPlugin({
            cleanStaleWebpackAssets: false,
            cleanOnceBeforeBuildPatterns: [path.resolve(__dirname, './dist')],
        }),
        // new WebpackBundleAnalyzer(),
    ],
    resolve: {
        extensions: ['.js'],
    },
};

module.exports = (env, argv) => {
    if (argv.mode === 'development') {
        // * add some development rules here
    } else if (argv.mode === 'production') {
        // * add some prod rules here
    } else {
        throw new Error('Specify env');
    }

    return config;
};
