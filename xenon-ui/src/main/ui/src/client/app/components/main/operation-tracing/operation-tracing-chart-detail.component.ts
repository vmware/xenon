// angular
import { ChangeDetectionStrategy, Input } from '@angular/core';
import * as _ from 'lodash';
import * as moment from 'moment';

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { StringUtil } from '../../../frameworks/app/utils/index';

@BaseComponent({
    selector: 'xe-operation-tracing-chart-detail',
    moduleId: module.id,
    templateUrl: './operation-tracing-chart-detail.component.html',
    styleUrls: ['./operation-tracing-chart-detail.component.css']
})

export class OperationTracingChartDetailComponent {
    @Input()
    event: {[key: string]: any};

    getTimeStamp(time: Date): string {
        return moment(time).format('M/D/YY hh:mm:ss.SSS A');
    }

    getJSONString(value: any): string {
        try {
            return JSON.stringify(value, null, 2);
        } catch(e) {
            return '';
        }
    }

    isNullOrUndefined(value: any) {
        return _.isNull(value) || _.isUndefined(value);
    }
}
