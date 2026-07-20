// @vitest-environment jsdom
import '@angular/compiler';
import * as ngCore from '@angular/core';
import { BrowserTestingModule, platformBrowserTesting } from '@angular/platform-browser/testing';
import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
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

const resolveComponentResources = (
  ngCore as typeof ngCore & {
    ɵresolveComponentResources: (resolver: (url: string) => Promise<string>) => Promise<void>;
  }
).ɵresolveComponentResources;

if (!TestBed.platform || !TestBed.ngModule) {
  getTestBed().initTestEnvironment(BrowserTestingModule, platformBrowserTesting(), {
    errorOnUnknownElements: true,
    errorOnUnknownProperties: true,
  });
}

describe('ArgentinaComponent', () => {
  let component: GwtArgentinaComponent;
  let fixture: ComponentFixture<GwtArgentinaComponent>;

  beforeEach(async () => {
    await resolveComponentResources(() => Promise.resolve(''));

    await TestBed.configureTestingModule({
      imports: [GwtArgentinaComponent],
      providers: [
        GwtArgentinaConfigService,
        { provide: BreakpointObserver, useValue: breakpointObserverMock },
        { provide: LocalStorageService, useValue: localStorageServiceMock },
      ],
    })
      .overrideComponent(GwtArgentinaComponent, {
        set: {
          template: '',
          styles: [],
          styleUrl: undefined,
          styleUrls: [],
        },
      })
      .compileComponents();

    fixture = TestBed.createComponent(GwtArgentinaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
