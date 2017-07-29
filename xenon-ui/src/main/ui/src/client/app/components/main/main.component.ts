// angular
import { Component } from '@angular/core';

// app
import { AnalyticsService } from '../../modules/analytics/index';

@Component({
    moduleId: module.id,
    selector: 'xe-main',
    templateUrl: 'main.component.html'
})

export class MainComponent {
    constructor(public analytics: AnalyticsService) {

    }
}
