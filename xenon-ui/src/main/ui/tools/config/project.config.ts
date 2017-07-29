import { join } from 'path';
import { SeedAdvancedConfig } from './seed-advanced.config';

/**
 * This class extends the basic seed configuration, allowing for project specific overrides. A few examples can be found
 * below.
 */
export class ProjectConfig extends SeedAdvancedConfig {

    PROJECT_TASKS_DIR = join(process.cwd(), this.TOOLS_DIR, 'tasks', 'project');

    FONTS_DEST = `${this.ASSETS_DEST}/fonts`;
    FONTS_SRC = [
        'node_modules/font-awesome/fonts/**',
        `${this.ASSETS_SRC}/fonts/**`
    ];

    constructor() {
        super();
        /* Enable typeless compiler runs (faster) between typed compiler runs. */
        // this.TYPED_COMPILE_INTERVAL = 5;

        // Add `NPM` third-party libraries to be injected/bundled.
        this.NPM_DEPENDENCIES = [
            ...this.NPM_DEPENDENCIES,
            { src: 'jquery/dist/jquery.min.js', inject: 'libs' },
            { src: 'tether/dist/js/tether.min.js', inject: 'libs' },
            { src: 'bootstrap/dist/js/bootstrap.min.js', inject: 'libs' },
            { src: 'd3/d3.min.js', inject: 'libs' },
            { src: 'd3-tip/index.js', inject: 'libs' },
            { src: `patternfly-timeline/dist/timeline.js`, inject: 'libs' },
            { src: 'chart.js/dist/Chart.bundle.min.js', inject: 'libs' },

            { src: 'codemirror/lib/codemirror.js', inject: 'libs' },
            { src: 'codemirror/addon/display/autorefresh.js', inject: 'libs' },
            { src: 'codemirror/addon/edit/matchbrackets.js', inject: 'libs' },
            { src: 'codemirror/addon/selection/active-line.js', inject: 'libs' },
            { src: 'codemirror/mode/javascript/javascript.js', inject: 'libs' },

            { src: `patternfly-timeline/dist/timeline.css`, inject: true },
            { src: 'codemirror/lib/codemirror.css', inject: true }
        ];

        // Add `local` third-party libraries to be injected/bundled.
        this.APP_ASSETS = [
            ...this.APP_ASSETS
        ];

        this.ROLLUP_INCLUDE_DIR = [
            ...this.ROLLUP_INCLUDE_DIR,
            //'node_modules/moment/**'
        ];

        this.ROLLUP_NAMED_EXPORTS = [
            ...this.ROLLUP_NAMED_EXPORTS,
            //{'node_modules/immutable/dist/immutable.js': [ 'Map' ]},
        ];

        /* Add proxy middleware */
        this.PROXY_MIDDLEWARE = [
            require('http-proxy-middleware')('/api', {
                target: 'http://localhost:8000',
                changeOrigin: true,
                ws: true,
                pathRewrite: {
                    '^/api': ''
                }
            })
        ];

        /* Add to or override NPM module configurations: */
        // this.PLUGIN_CONFIGS['browser-sync'] = { ghostMode: false };
    }

}
