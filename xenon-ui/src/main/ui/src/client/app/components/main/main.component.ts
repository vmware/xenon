// angular
import { ChangeDetectionStrategy } from '@angular/core';

// app
import { AnalyticsService } from '../../frameworks/analytics/index';
import { BaseComponent } from '../../frameworks/core/index';

@BaseComponent({
    moduleId: module.id,
    selector: 'xe-main',
    templateUrl: 'main.component.html',
    changeDetection: ChangeDetectionStrategy.Default // Everything else uses OnPush
})

export class MainComponent {
    constructor(public analytics: AnalyticsService) {

    }
}
