import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BreakpointObserver } from '@angular/cdk/layout';
import { of } from 'rxjs';
import { beforeEach, describe, expect, it } from 'vitest';

import { GwtNewZealandConfigService } from './gwt-new-zealand-config.service';
import { GwtNewZealandComponent } from './gwt-new-zealand.component';
import { LocalStorageService } from '../shared/local-storage.service';

const breakpointObserverMock = {
  observe: () => of({ matches: false }),
};

const localStorageServiceMock = {
  getNumber: () => null,
  setNumber: () => undefined,
};

describe('GwtNewZealandComponent', () => {
  let component: GwtNewZealandComponent;
  let fixture: ComponentFixture<GwtNewZealandComponent>;

  beforeEach(async () => {
    TestBed.overrideComponent(GwtNewZealandComponent, {
      set: {
        template: '',
      },
    });

    await TestBed.configureTestingModule({
      imports: [GwtNewZealandComponent],
      providers: [
        GwtNewZealandConfigService,
        { provide: BreakpointObserver, useValue: breakpointObserverMock },
        { provide: LocalStorageService, useValue: localStorageServiceMock },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(GwtNewZealandComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
