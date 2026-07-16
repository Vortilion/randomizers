import { ComponentFixture, TestBed } from '@angular/core/testing';
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

describe('SecondEditionComponent', () => {
  let component: GwtSecondEditionComponent;
  let fixture: ComponentFixture<GwtSecondEditionComponent>;

  beforeEach(async () => {
    TestBed.overrideComponent(GwtSecondEditionComponent, {
      set: {
        template: '',
      },
    });

    await TestBed.configureTestingModule({
      imports: [GwtSecondEditionComponent],
      providers: [
        GwtSecondEditionConfigService,
        { provide: BreakpointObserver, useValue: breakpointObserverMock },
        { provide: LocalStorageService, useValue: localStorageServiceMock },
        { provide: MatDialog, useValue: matDialogMock },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(GwtSecondEditionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
