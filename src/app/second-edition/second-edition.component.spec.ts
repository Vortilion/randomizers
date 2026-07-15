import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BreakpointObserver } from '@angular/cdk/layout';
import { MatDialog } from '@angular/material/dialog';
import { of } from 'rxjs';
import { beforeEach, describe, expect, it } from 'vitest';

import { SecondEditionComponent } from './second-edition.component';
import { LocalStorageService } from '../shared/local-storage.service';
import { SecondEditionConfigService } from './second-edition-config.service';

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

describe('SecondEditionComponent', () => {
  let component: SecondEditionComponent;
  let fixture: ComponentFixture<SecondEditionComponent>;

  beforeEach(async () => {
    TestBed.overrideComponent(SecondEditionComponent, {
      set: {
        template: '',
      },
    });

    await TestBed.configureTestingModule({
      imports: [SecondEditionComponent],
      providers: [
        SecondEditionConfigService,
        { provide: BreakpointObserver, useValue: breakpointObserverMock },
        { provide: LocalStorageService, useValue: localStorageServiceMock },
        { provide: MatDialog, useValue: matDialogMock },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(SecondEditionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
