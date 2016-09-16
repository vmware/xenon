// angular
import { ChangeDetectionStrategy } from '@angular/core';

// app
import { BaseComponent } from '../frameworks/core/index';

import { AnalyticsService } from '../frameworks/analytics/index';
import { MultilingualService } from '../frameworks/i18n/index';

@BaseComponent({
    moduleId: module.id,
    selector: 'xe-app',
    templateUrl: 'app.component.html',
    changeDetection: ChangeDetectionStrategy.Default // Everything else uses OnPush
})

export class AppComponent {
    constructor(
        public analytics: AnalyticsService,
        private _multilang: MultilingualService) {}
}
