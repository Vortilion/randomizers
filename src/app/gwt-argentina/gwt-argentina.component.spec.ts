import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BreakpointObserver } from '@angular/cdk/layout';
import { of } from 'rxjs';
import { beforeEach, describe, expect, it } from 'vitest';

import { GwtArgentinaConfigService } from './gwt-argentina-config.service';
import { GwtArgentinaComponent } from './gwt-argentina.component';
import { LocalStorageService } from '../shared/local-storage.service';

const breakpointObserverMock = {
  observe: () => of({ matches: false }),
};

const localStorageServiceMock = {
  getNumber: () => null,
  setNumber: () => undefined,
};

describe('ArgentinaComponent', () => {
  let component: GwtArgentinaComponent;
  let fixture: ComponentFixture<GwtArgentinaComponent>;

  beforeEach(async () => {
    TestBed.overrideComponent(GwtArgentinaComponent, {
      set: {
        template: '',
      },
    });

    await TestBed.configureTestingModule({
      imports: [GwtArgentinaComponent],
      providers: [
        GwtArgentinaConfigService,
        { provide: BreakpointObserver, useValue: breakpointObserverMock },
        { provide: LocalStorageService, useValue: localStorageServiceMock },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(GwtArgentinaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
