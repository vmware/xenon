// angular
import { ChangeDetectionStrategy, Component, AfterViewInit, Input, Output, ViewChild,
    EventEmitter, forwardRef } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import * as _ from 'lodash';

declare var CodeMirror: any;

@Component({
    selector: 'xe-code-editor',
    moduleId: module.id,
    templateUrl: './code-editor.component.html',
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => CodeEditorComponent),
            multi: true
        }
    ]
})

export class CodeEditorComponent implements AfterViewInit {
    @ViewChild('textarea')
    textarea;

    /**
     * CodeMirror config object that provide customizations to the code editor.
     */
    @Input()
    config: any;

    /**
     * Code editor value.
     */
    @Input()
    value: string;

    /**
     * Emit event when change happens.
     */
    @Output()
    change = new EventEmitter<string>();

    /**
     * CodeMirror instance.
     */
    private instance: any;

    ngAfterViewInit(): void {
        // Override default properties with config properties
        this.config = _.assignIn({
            lineNumbers: true,
            lineWrapping: false,
            autoRefresh: true,
            styleActiveLine: true,
            fixedGutter: true,
            matchBrackets: true,
            coverGutterNextToScrollbar: false,
            indentUnit: 0,
            tabSize: 4,
            smartIndent: true,
            theme: 'mdn-like',
            mode: 'javascript'
        }, this.config);
        this.instance = CodeMirror.fromTextArea(this.textarea.nativeElement, this.config);
        this.instance.on('change', () => {
            this.updateValue(this.instance.getValue());
        });
    }

    /**
     * Value update process
     */
    updateValue(value): void {
        this.value = value;
        this.onChanged(value);
        this.onTouched();
    }

    /**
     * Implements ControlValueAccessor
     */
    writeValue(value): void {
        this.value = value || '';
        if (this.instance) {
            this.instance.setValue(this.value);
        }
    }

    onChanged(value): any {
        this.change.emit(value);
    }

    onTouched() {
        // do nothing
    }

    registerOnChange(fn) {
        this.onChanged = fn;
    }

    registerOnTouched(fn) {
        this.onTouched = fn;
    }
}
