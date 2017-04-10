import { AfterViewInit } from '@angular/core';

import { BaseComponent } from '../../frameworks/core/index';

@BaseComponent({
    selector: 'xe-star-canvas',
    moduleId: module.id,
    templateUrl: './star-canvas.component.html'
})

/**
 * A standlone component that renders floating stars on a canvas
 */
export class StarCanvasComponent implements AfterViewInit {
    /**
     * The canvas to be rendered on
     */
    private _canvas: any;

    /**
     * The 2D context of the canvas
     */
    private _context: any;

    /**
     * Rendering options
     */
    private _options: any = {
        starCount: 250,
        distance: 70,
        d_radius: 150,
        array: []
    };

    /**
     * Rendering origin
     */
    private _origin: any;

    /**
     * Initiate the basic DOM related variables after DOM is fully rendered.
     */
    ngAfterViewInit() {
        this._canvas = document.querySelector('canvas');
        this._context = this._canvas.getContext('2d');
        this._origin = {
            x: 30 * this._canvas.width / 100,
            y: 30 * this._canvas.height / 100
        };

        this._canvas.width = this._canvas.parentElement.offsetWidth;
        this._canvas.height = this._canvas.parentElement.offsetHeight;
        this._canvas.style.display = 'block';

        var devicePixelRatio = window.devicePixelRatio || 1;
        var backingStoreRatio = this._context.webkitBackingStorePixelRatio ||
                this._context.mozBackingStorePixelRatio ||
                this._context.msBackingStorePixelRatio ||
                this._context.oBackingStorePixelRatio ||
                this._context.backingStorePixelRatio || 1;
        var ratio = devicePixelRatio / backingStoreRatio;

        if (devicePixelRatio !== backingStoreRatio) {
            var oldWidth = this._canvas.width;
            var oldHeight = this._canvas.height;

            this._canvas.width = oldWidth * ratio;
            this._canvas.height = oldHeight * ratio;

            this._canvas.style.width = oldWidth + 'px';
            this._canvas.style.height = oldHeight + 'px';

            // now scale the context to counter
            // the fact that we've manually scaled
            // our canvas element
            this._context.scale(ratio, ratio);
        }

        this._context.fillStyle = 'rgba(41, 182, 246, .5)';
        this._context.lineWidth = .5;
        this._context.strokeStyle = 'rgba(41, 182, 246, .25)';

        setInterval(() => {
            this._render();
        }, 1000 / 30);
    }

    onMouseMove(event: MouseEvent): void {
        if (event.type) {
            this._origin.x = event.offsetX;
            this._origin.y = event.offsetY;
        }
    }

    onMouseLeave(event: MouseEvent): void {
        if (event.type) {
            this._origin.x = this._canvas.width / 2;
            this._origin.y = this._canvas.height / 2;
        }
    }

    private _render(): void {
        this._context.clearRect(0, 0, this._canvas.width, this._canvas.height);

        var star: any;
        var stars: any[] = this._options.array;

        for (let i: number = 0; i < this._options.starCount; i++) {
            stars.push((new Star(this._canvas, this._context, this._origin, this._options)));
            star = this._options.array[i];
            star.create();
        }

        star.line();
        star.animate();
    }
}

/**
 * A star which will be renderred on StarCanvasComponent
 */
class Star {
    /**
     * The x coordiate
     */
    public x: number;

    /**
     * The y coordinate
     */
    public y: number;

    /**
     * The width of the star
     */
    public width: number;

    /**
     * The height of the star
     */
    public height: number;

    /**
     * The star's rendering radius
     */
    public radius: number;

    constructor(private _canvas: any,
            private _context: any,
            private _origin: any,
            private _options: any) {
        this.x = Math.random() * _canvas.width;
        this.y = Math.random() * _canvas.height;
        this.width = -1 + Math.random();
        this.height = -1 + Math.random();
        this.radius = .5 + 3.5 * Math.random();
    }

    /**
     * Create/render the star
     */
    create(): void {
        this._context.beginPath();
        this._context.arc(this.x, this.y, this.radius, 0, 2 * Math.PI, !1);
        this._context.fill();
    }

    /**
     * Animate the star's movements
     */
    animate(): void {
        for (let i = 0; i < this._options.starCount; i++) {
            var star = this._options.array[i];

            if (star.y < 0 || star.y > this._canvas.height) {
                star.width = star.width;
                star.height = -star.height;
            } else if (star.x < 0 || star.x > this._canvas.width) {
                star.width = -star.width;
                star.height = star.height;
            }
            star.x += star.width;
            star.y += star.height;
        }
    }

    /**
     * Draw line between the stars
     */
    line(): void {
        for (let i: number = 0; i < this._options.starCount; i++) {
            for (let j: number = 0; j < this._options.starCount; j++) {
                let iStar = this._options.array[i];
                let jStar = this._options.array[j];

                if (iStar.x - jStar.x < this._options.distance &&
                        iStar.y - jStar.y < this._options.distance &&
                        iStar.x - jStar.x > -this._options.distance &&
                        iStar.y - jStar.y > -this._options.distance &&
                        iStar.x - this._origin.x < this._options.d_radius &&
                        iStar.y - this._origin.y < this._options.d_radius &&
                        iStar.x - this._origin.x > -this._options.d_radius &&
                        iStar.y - this._origin.y > -this._options.d_radius) {
                    this._context.beginPath();
                    this._context.moveTo(iStar.x, iStar.y);
                    this._context.lineTo(jStar.x, jStar.y);
                    this._context.stroke();
                    this._context.closePath();
                }
            }
        }
    }
}
