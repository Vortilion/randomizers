import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';
import { BreakpointObserver } from '@angular/cdk/layout';
import { beforeEach, describe, expect, it } from 'vitest';

import { PageHeaderComponent } from './page-header.component';

const breakpointObserverMock = {
    observe: () => of({ matches: false }),
};

describe('PageHeaderComponent', () => {
    let component: PageHeaderComponent;
    let fixture: ComponentFixture<PageHeaderComponent>;

    beforeEach(async () => {
        TestBed.overrideComponent(PageHeaderComponent, {
            set: {
                template: '',
            },
        });

        await TestBed.configureTestingModule({
            imports: [PageHeaderComponent],
            providers: [{ provide: BreakpointObserver, useValue: breakpointObserverMock }],
        }).compileComponents();

        fixture = TestBed.createComponent(PageHeaderComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
