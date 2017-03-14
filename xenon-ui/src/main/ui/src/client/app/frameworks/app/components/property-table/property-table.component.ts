// angular
import { Input } from '@angular/core';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../core/index';

import { StringUtil } from '../../utils/index';

@BaseComponent({
    selector: 'xe-property-table',
    moduleId: module.id,
    templateUrl: './property-table.component.html',
    styleUrls: ['./property-table.component.css']
})

export class PropertyTableComponent {
    @Input()
    properties: {[key: string]: any};

    @Input()
    hasHeader: boolean = false;

    @Input()
    bordered: boolean = false;

    getPropertyArray(): any[] {
        var propertyArray: any[] = [];

        if (_.isUndefined(this.properties) || _.isNull(this.properties)) {
            return propertyArray;
        }

        _.each(this.properties, (value: any, key: string) => {
            var valueType: string = this._getValueType(key, value);

            propertyArray.push({
                key: key,
                value: valueType === 'object' ? JSON.stringify(value, null, 2) : value,
                valueType: valueType
            });
        });

        return propertyArray;
    }

    formatTimeStamp(milliseconds: number): string {
        return StringUtil.getTimeStamp(milliseconds);
    }

    private _getValueType(key: string, value: any): string {
        if (_.endsWith(key.toLowerCase(), 'link')) {
            return 'link';
        }

        if (_.isNumber(value)) {
            if (key && _.endsWith(key.toLowerCase(), 'timemicros')) {
                return 'date';
            } else {
                return 'number';
            }
        }

        if (_.isObject(value)) {
            return 'object';
        }

        return 'string';
    }
}
