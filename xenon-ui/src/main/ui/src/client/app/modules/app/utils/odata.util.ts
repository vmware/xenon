export class ODataUtil {
    static DEFAULT_PAGE_SIZE: number = 25;
    static PAGE_LIMIT: number = 10000;

    static DEFAULT_LOG_SIZE: number = 1000;

    static EXPAND: string = '$expand';
    static FILTER: string = '$filter';
    static SKIP: string = '$skip';
    static ORDER_BY: string = '$orderby';
    static ORDER_BY_TYPE: string = '$orderbytype';
    static ORDER_BY_VALUE_ASC: string = 'asc';
    static ORDER_BY_VALUE_DESC: string = 'desc';
    static TOP: string = '$top';
    static LIMIT: string = '$limit';
    static COUNT: string = '$count';
    static SKIP_TO: string = '$skipto';
    static NODE: string = '$nodeid';
    static TENANTLINKS: string = '$tenantLinks';

    static pageLimit(): string {
        return `${this.LIMIT}=${this.PAGE_LIMIT}`;
    }

    static limit(pageSize: number = this.DEFAULT_PAGE_SIZE): string {
        return `${this.LIMIT}=${pageSize}`;
    }

    static count(): string {
        return `${this.COUNT}=true`;
    }

    static orderBy(propertyName: string, order: string = 'asc') {
        return `${this.ORDER_BY}=${propertyName} ${order}`;
    }
}
