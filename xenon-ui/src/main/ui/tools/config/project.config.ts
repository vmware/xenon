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

        // Add `NPM` third-party libraries to be injected/bundled.
        this.NPM_DEPENDENCIES = [
            ...this.NPM_DEPENDENCIES,
            { src: 'jquery/dist/jquery.min.js', inject: 'libs' },
            { src: 'tether/dist/js/tether.min.js', inject: 'libs' },
            { src: 'bootstrap/dist/js/bootstrap.min.js', inject: 'libs' },
            { src: 'd3/d3.min.js', inject: 'libs' },
            { src: 'd3-tip/index.js', inject: 'libs' },
            { src: 'chart.js/dist/Chart.bundle.min.js', inject: 'libs' }
        ];

        // Add `local` third-party libraries to be injected/bundled.
        this.APP_ASSETS = [
            ...this.APP_ASSETS,
            { src: `${this.ASSETS_SRC}/main.scss`, inject: true },
        ];

        /* Add to or override NPM module configurations: */
        // this.mergeObject(this.PLUGIN_CONFIGS['browser-sync'], { ghostMode: false });
    }

}
