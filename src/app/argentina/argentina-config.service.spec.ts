import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ArgentinaConfigService } from './argentina-config.service';

describe('ArgentinaConfigService', () => {
  let service: ArgentinaConfigService;

  beforeEach(() => {
    service = new ArgentinaConfigService();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should create', () => {
    expect(service).toBeTruthy();
  });

  describe('playerCount', () => {
    it('should emit player counts', () => {
      let emitted: number | null = null;

      service.playerCount.subscribe((value) => {
        emitted = value;
      });

      service.playerCount.emit(3);

      expect(emitted).toBe(3);
    });

    it('should notify multiple player count subscribers', () => {
      const emitted: number[] = [];

      service.playerCount.subscribe((value) => emitted.push(value));
      service.playerCount.subscribe((value) => emitted.push(value * 10));

      service.playerCount.emit(2);

      expect(emitted).toEqual([2, 20]);
    });
  });

  describe('getRandomNeutralBuildingOrder', () => {
    it('should return shuffled neutral buildings without mutating source', () => {
      const sourceTitles = service.neutralBuildings.map((tile) => tile.title);

      vi.spyOn(Math, 'random').mockReturnValue(0.5);
      const result = service.getRandomNeutralBuildingOrder();

      expect(result).toHaveLength(service.neutralBuildings.length);
      expect(result.map((tile) => tile.title).sort()).toEqual([...sourceTitles].sort());
      expect(service.neutralBuildings.map((tile) => tile.title)).toEqual(sourceTitles);
      expect(result).not.toBe(service.neutralBuildings);
    });
  });

  describe('getRandomStationMasters', () => {
    it('should select five unique station masters', () => {
      vi.spyOn(Math, 'random').mockReturnValue(0.1);

      const result = service.getRandomStationMasters();
      const titles = result.map((tile) => tile.title);

      expect(result).toHaveLength(5);
      expect(new Set(titles).size).toBe(5);
      expect(service.stationMasters).toHaveLength(8);
    });

    it('should not mutate station master source order', () => {
      const sourceTitles = service.stationMasters.map((tile) => tile.title);

      vi.spyOn(Math, 'random').mockReturnValue(0.9);
      service.getRandomStationMasters();

      expect(service.stationMasters.map((tile) => tile.title)).toEqual(sourceTitles);
    });
  });

  describe('getRandomPlayerBuildings', () => {
    it('should return player buildings with exactly one side each and keep source intact', () => {
      vi.spyOn(Math, 'random').mockReturnValue(0);

      const result = service.getRandomPlayerBuildings();

      expect(result).toHaveLength(service.playerBuildings.length);
      expect(result.every((tile) => tile.sides.length === 1)).toBe(true);
      expect(service.playerBuildings.every((tile) => tile.sides.length === 2)).toBe(true);
    });

    it('should keep original player building side data by deep cloning', () => {
      vi.spyOn(Math, 'random').mockReturnValue(0.999);

      const result = service.getRandomPlayerBuildings();

      expect(result.every((tile) => tile.sides[0].title === 'a')).toBe(true);
      expect(result[0]).not.toBe(service.playerBuildings[0]);
      expect(result[0].sides[0]).not.toBe(service.playerBuildings[0].sides[0]);
      expect(service.playerBuildings.every((tile) => tile.sides.map((side) => side.title).join(',') === 'a,b')).toBe(true);
    });
  });

  describe('getRandomCities', () => {
    it('should return cities with exactly one side each and keep source intact', () => {
      vi.spyOn(Math, 'random').mockReturnValue(0);

      const result = service.getRandomCities();

      expect(result).toHaveLength(service.cities.length);
      expect(result.every((tile) => tile.sides.length === 1)).toBe(true);
      expect(service.cities.every((tile) => tile.sides.length === 2)).toBe(true);
    });

    it('should support selecting second city side and keep originals untouched', () => {
      vi.spyOn(Math, 'random').mockReturnValue(0.999);

      const result = service.getRandomCities();

      expect(result.every((tile) => tile.sides[0].title === 'a')).toBe(true);
      expect(result[0]).not.toBe(service.cities[0]);
      expect(service.cities.every((tile) => tile.sides.map((side) => side.title).join(',') === 'a,b')).toBe(true);
    });
  });
});
