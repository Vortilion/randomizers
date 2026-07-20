// @vitest-environment jsdom
import '@angular/compiler';
import * as ngCore from '@angular/core';
import { BrowserTestingModule, platformBrowserTesting } from '@angular/platform-browser/testing';
import { TestBed, ɵgetCleanupHook as getCleanupHook } from '@angular/core/testing';
import { RouterModule } from '@angular/router';
import { MatSnackBar } from '@angular/material/snack-bar';
import { SwUpdate } from '@angular/service-worker';
import { provideTransloco } from '@jsverse/transloco';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { App } from './app';

const resolveComponentResources = (
  ngCore as typeof ngCore & {
    ɵresolveComponentResources: (resolver: (url: string) => Promise<string>) => Promise<void>;
  }
).ɵresolveComponentResources;

if (!TestBed.platform || !TestBed.ngModule) {
  TestBed.initTestEnvironment(BrowserTestingModule, platformBrowserTesting(), {
    errorOnUnknownElements: true,
    errorOnUnknownProperties: true,
  });
}

beforeEach(getCleanupHook(false));
afterEach(getCleanupHook(true));

describe('App', () => {
  beforeEach(async () => {
    await resolveComponentResources(() => Promise.resolve(''));

    await TestBed.configureTestingModule({
      imports: [RouterModule.forRoot([]), App],
      providers: [
        provideTransloco({
          config: {
            availableLangs: ['de', 'en', 'pl'],
            defaultLang: 'en',
            fallbackLang: 'en',
          },
        }),
        {
          provide: SwUpdate,
          useValue: {
            unrecoverable: { subscribe: () => void 0 },
            versionUpdates: { pipe: () => ({ subscribe: () => void 0 }) },
          },
        },
        {
          provide: MatSnackBar,
          useValue: {
            open: () => ({ onAction: () => ({ subscribe: () => void 0 }) }),
          },
        },
      ],
    })
      .overrideComponent(App, {
        set: {
          template: '<router-outlet></router-outlet>',
          styles: [],
          styleUrl: undefined,
        },
      })
      .compileComponents();
  });

  it('should create the app', () => {
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    expect(app).toBeTruthy();
  });

  it(`should have as title 'gwt-randomizers'`, () => {
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    expect(app.title).toEqual('gwt-randomizers');
  });

  it('should render title', async () => {
    const fixture = TestBed.createComponent(App);
    await fixture.whenStable();
    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.querySelector('router-outlet')).not.toBeNull();
  });
});
