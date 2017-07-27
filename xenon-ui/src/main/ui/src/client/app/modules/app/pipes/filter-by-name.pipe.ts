import { Pipe, PipeTransform } from '@angular/core';
import * as _ from 'lodash';

@Pipe({ name: 'filterByName' })
export class FilterByNamePipe implements PipeTransform {

    transform(value: any, name: string = ''): any {
        if (_.isEmpty(value) || name === '') {
            return value;
        }

        return value.filter((item: any) => {
            if (_.isString(item)) {
                return item.toLowerCase().indexOf(name.toLowerCase()) > -1;
            }

            return item.name ? item.name.toLowerCase().indexOf(name.toLowerCase()) > -1 : false;
        });
    }
}
