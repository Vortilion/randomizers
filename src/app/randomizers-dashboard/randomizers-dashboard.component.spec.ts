import { ComponentFixture, TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from 'vitest';

import { RandomizersDashboardComponent } from './randomizers-dashboard.component';

describe('RandomizersDashboardComponent', () => {
  let component: RandomizersDashboardComponent;
  let fixture: ComponentFixture<RandomizersDashboardComponent>;

  beforeEach(async () => {
    TestBed.overrideComponent(RandomizersDashboardComponent, {
      set: {
        template: '',
      },
    });

    await TestBed.configureTestingModule({
      imports: [RandomizersDashboardComponent],
    }).compileComponents();

    fixture = TestBed.createComponent(RandomizersDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
