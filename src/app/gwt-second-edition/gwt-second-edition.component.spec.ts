// @vitest-environment jsdom
import '@angular/compiler';
import * as ngCore from '@angular/core';
import { BrowserTestingModule, platformBrowserTesting } from '@angular/platform-browser/testing';
import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import { BreakpointObserver } from '@angular/cdk/layout';
import { MatDialog } from '@angular/material/dialog';
import { of } from 'rxjs';
import { beforeEach, describe, expect, it } from 'vitest';

import { GwtSecondEditionComponent } from './gwt-second-edition.component';
import { LocalStorageService } from '../shared/local-storage.service';
import { GwtSecondEditionConfigService } from './gwt-second-edition-config.service';

const breakpointObserverMock = {
  observe: () => of({ matches: false }),
};

const localStorageServiceMock = {
  get: () => null,
  set: () => true,
};

const matDialogMock = {
  open: () => ({
    afterClosed: () => of(undefined),
  }),
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

describe('SecondEditionComponent', () => {
  let component: GwtSecondEditionComponent;
  let fixture: ComponentFixture<GwtSecondEditionComponent>;

  beforeEach(async () => {
    await resolveComponentResources(() => Promise.resolve(''));

    await TestBed.configureTestingModule({
      imports: [GwtSecondEditionComponent],
      providers: [
        GwtSecondEditionConfigService,
        { provide: BreakpointObserver, useValue: breakpointObserverMock },
        { provide: LocalStorageService, useValue: localStorageServiceMock },
        { provide: MatDialog, useValue: matDialogMock },
      ],
    })
      .overrideComponent(GwtSecondEditionComponent, {
        set: {
          template: '',
          styles: [],
          styleUrl: undefined,
          styleUrls: [],
        },
      })
      .compileComponents();

    fixture = TestBed.createComponent(GwtSecondEditionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
