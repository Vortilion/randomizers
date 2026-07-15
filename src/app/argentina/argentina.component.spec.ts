import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BreakpointObserver } from '@angular/cdk/layout';
import { of } from 'rxjs';
import { beforeEach, describe, expect, it } from 'vitest';

import { ArgentinaConfigService } from './argentina-config.service';
import { ArgentinaComponent } from './argentina.component';
import { LocalStorageService } from '../shared/local-storage.service';

const breakpointObserverMock = {
  observe: () => of({ matches: false }),
};

const localStorageServiceMock = {
  getNumber: () => null,
  setNumber: () => undefined,
};

describe('ArgentinaComponent', () => {
  let component: ArgentinaComponent;
  let fixture: ComponentFixture<ArgentinaComponent>;

  beforeEach(async () => {
    TestBed.overrideComponent(ArgentinaComponent, {
      set: {
        template: '',
      },
    });

    await TestBed.configureTestingModule({
      imports: [ArgentinaComponent],
      providers: [
        ArgentinaConfigService,
        { provide: BreakpointObserver, useValue: breakpointObserverMock },
        { provide: LocalStorageService, useValue: localStorageServiceMock },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ArgentinaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
