import { AfterViewInit, ChangeDetectionStrategy, Component } from '@angular/core';

@Component({
    selector: 'xe-star-canvas',
    moduleId: module.id,
    templateUrl: './star-canvas.component.html',
    changeDetection: ChangeDetectionStrategy.OnPush
})

/**
 * A standlone component that renders floating stars on a canvas
 */
export class StarCanvasComponent implements AfterViewInit {
    /**
     * The canvas to be rendered on
     */
    private canvas: any;

    /**
     * The 2D context of the canvas
     */
    private context: any;

    /**
     * Rendering options
     */
    private options: any = {
        starCount: 250,
        distance: 70,
        d_radius: 150,
        array: []
    };

    /**
     * Rendering origin
     */
    private origin: any;

    /**
     * Initiate the basic DOM related variables after DOM is fully rendered.
     */
    ngAfterViewInit() {
        this.canvas = document.querySelector('canvas');
        this.context = this.canvas.getContext('2d');
        this.origin = {
            x: 30 * this.canvas.width / 100,
            y: 30 * this.canvas.height / 100
        };

        this.canvas.width = this.canvas.parentElement.offsetWidth;
        this.canvas.height = this.canvas.parentElement.offsetHeight;
        this.canvas.style.display = 'block';

        var devicePixelRatio = window.devicePixelRatio || 1;
        var backingStoreRatio = this.context.webkitBackingStorePixelRatio ||
                this.context.mozBackingStorePixelRatio ||
                this.context.msBackingStorePixelRatio ||
                this.context.oBackingStorePixelRatio ||
                this.context.backingStorePixelRatio || 1;
        var ratio = devicePixelRatio / backingStoreRatio;

        if (devicePixelRatio !== backingStoreRatio) {
            var oldWidth = this.canvas.width;
            var oldHeight = this.canvas.height;

            this.canvas.width = oldWidth * ratio;
            this.canvas.height = oldHeight * ratio;

            this.canvas.style.width = oldWidth + 'px';
            this.canvas.style.height = oldHeight + 'px';

            // now scale the context to counter
            // the fact that we've manually scaled
            // our canvas element
            this.context.scale(ratio, ratio);
        }

        this.context.fillStyle = 'rgba(41, 182, 246, .5)';
        this.context.lineWidth = .5;
        this.context.strokeStyle = 'rgba(41, 182, 246, .25)';

        setInterval(() => {
            this.render();
        }, 1000 / 30);
    }

    onMouseMove(event: MouseEvent): void {
        if (event.type) {
            this.origin.x = event.offsetX;
            this.origin.y = event.offsetY;
        }
    }

    onMouseLeave(event: MouseEvent): void {
        if (event.type) {
            this.origin.x = this.canvas.width / 2;
            this.origin.y = this.canvas.height / 2;
        }
    }

    private render(): void {
        this.context.clearRect(0, 0, this.canvas.width, this.canvas.height);

        var star: any;
        var stars: any[] = this.options.array;

        for (let i: number = 0; i < this.options.starCount; i++) {
            stars.push((new Star(this.canvas, this.context, this.origin, this.options)));
            star = this.options.array[i];
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

    constructor(private canvas: any,
            private context: any,
            private origin: any,
            private options: any) {
        this.x = Math.random() * canvas.width;
        this.y = Math.random() * canvas.height;
        this.width = -1 + Math.random();
        this.height = -1 + Math.random();
        this.radius = .5 + 3.5 * Math.random();
    }

    /**
     * Create/render the star
     */
    create(): void {
        this.context.beginPath();
        this.context.arc(this.x, this.y, this.radius, 0, 2 * Math.PI, !1);
        this.context.fill();
    }

    /**
     * Animate the star's movements
     */
    animate(): void {
        for (let i = 0; i < this.options.starCount; i++) {
            var star = this.options.array[i];

            if (star.y < 0 || star.y > this.canvas.height) {
                star.width = star.width;
                star.height = -star.height;
            } else if (star.x < 0 || star.x > this.canvas.width) {
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
        for (let i: number = 0; i < this.options.starCount; i++) {
            for (let j: number = 0; j < this.options.starCount; j++) {
                let iStar = this.options.array[i];
                let jStar = this.options.array[j];

                if (iStar.x - jStar.x < this.options.distance &&
                        iStar.y - jStar.y < this.options.distance &&
                        iStar.x - jStar.x > -this.options.distance &&
                        iStar.y - jStar.y > -this.options.distance &&
                        iStar.x - this.origin.x < this.options.d_radius &&
                        iStar.y - this.origin.y < this.options.d_radius &&
                        iStar.x - this.origin.x > -this.options.d_radius &&
                        iStar.y - this.origin.y > -this.options.d_radius) {
                    this.context.beginPath();
                    this.context.moveTo(iStar.x, iStar.y);
                    this.context.lineTo(jStar.x, jStar.y);
                    this.context.stroke();
                    this.context.closePath();
                }
            }
        }
    }
}
