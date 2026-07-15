import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class LocalStorageService {
  get<T>(key: string): T | null {
    if (!this.isStorageAvailable()) {
      return null;
    }

    const value = this.getString(key);

    if (value === null) {
      return null;
    }

    try {
      return JSON.parse(value) as T;
    } catch {
      return value as T;
    }
  }

  set<T>(key: string, value: T): boolean {
    if (!this.isStorageAvailable()) {
      return false;
    }

    try {
      const serialized = typeof value === 'string' ? value : JSON.stringify(value);
      globalThis.localStorage.setItem(key, serialized);
      return true;
    } catch {
      return false;
    }
  }

  delete(key: string): boolean {
    if (!this.isStorageAvailable()) {
      return false;
    }

    try {
      globalThis.localStorage.removeItem(key);
      return true;
    } catch {
      return false;
    }
  }

  clear(): boolean {
    if (!this.isStorageAvailable()) {
      return false;
    }

    try {
      globalThis.localStorage.clear();
      return true;
    } catch {
      return false;
    }
  }

  getNumber(key: string): number | null {
    const value = this.getString(key);

    if (value === null) {
      return null;
    }

    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  setNumber(key: string, value: number): void {
    this.setString(key, String(value));
  }

  getString(key: string): string | null {
    if (!this.isStorageAvailable()) {
      return null;
    }

    return globalThis.localStorage.getItem(key);
  }

  setString(key: string, value: string): void {
    if (!this.isStorageAvailable()) {
      return;
    }

    globalThis.localStorage.setItem(key, value);
  }

  private isStorageAvailable(): boolean {
    if (typeof globalThis === 'undefined' || !('localStorage' in globalThis)) {
      return false;
    }

    return globalThis.localStorage !== undefined && globalThis.localStorage !== null;
  }
}
