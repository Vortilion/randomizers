import { CommonModule } from '@angular/common';
import { Component, OnInit, inject } from '@angular/core';
import {
  TranslocoService,
  TranslocoPipe,
  getBrowserLang,
} from '@jsverse/transloco';
import { MaterialModule } from '../material/material.module';

@Component({
  selector: 'app-language-selector',
  standalone: true,
  imports: [CommonModule, MaterialModule, TranslocoPipe],
  templateUrl: './language-selector.component.html',
  styleUrls: ['./language-selector.component.scss'],
})
export class LanguageSelectorComponent implements OnInit {
  private translocoService = inject(TranslocoService);

  activeLang!: string;
  availableLangs!: ReturnType<TranslocoService['getAvailableLangs']>;

  ngOnInit(): void {
    const browserLang = getBrowserLang();

    this.availableLangs = this.translocoService.getAvailableLangs();

    if (browserLang && this.translocoService.isLang(browserLang)) {
      this.activeLang = browserLang;
      this.translocoService.setActiveLang(this.activeLang);
    } else {
      this.activeLang = this.translocoService.getDefaultLang();
    }
  }

  changeLanguage(lang: string): void {
    this.translocoService.setActiveLang(lang);
    this.activeLang = this.translocoService.getActiveLang();
  }
}
