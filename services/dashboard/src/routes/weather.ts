import { Router, Request, Response } from "express";
import { QueryApi } from "@influxdata/influxdb-client";

export function createWeatherRouter(queryApi: QueryApi, bucket: string): Router {
  const router = Router();

  // GET /api/weather/latest?city=London  (city is optional)
  router.get("/latest", async (req: Request, res: Response) => {
    try {
      const rawCity = req.query.city as string | undefined;
      // Sanitize: allow only alphanumeric, spaces, hyphens, underscores, periods.
      // Prevents Flux query injection via crafted city param.
      const city = rawCity ? rawCity.replace(/[^a-zA-Z0-9 _\-\.]/g, "") : undefined;

      let flux = `
        from(bucket: "${bucket}")
          |> range(start: -2h)
          |> filter(fn: (r) => r._measurement == "weather")
          |> filter(fn: (r) => r._field == "temperature")
      `;

      if (city) {
        flux += `  |> filter(fn: (r) => r.city_name == "${city}")\n`;
      }

      flux += `
          |> group(columns: ["city_name"])
          |> last()
      `;

      const rows: any[] = [];
      await new Promise<void>((resolve, reject) => {
        queryApi.queryRows(flux, {
          next(row, tableMeta) {
            const obj = tableMeta.toObject(row);
            rows.push({
              city_name:         obj.city_name         ?? "unknown",
              temperature:       obj._value             ?? 0,
              weather_condition: obj.weather_condition  ?? "unknown",
              recorded_at:       obj._time              ?? null,
            });
          },
          error: reject,
          complete: resolve,
        });
      });

      res.json(rows);
    } catch (err: any) {
      console.error("[weather] latest error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/weather/summary
  router.get("/summary", async (_req: Request, res: Response) => {
    try {
      // Get temperature stats from the last hour
      const statFlux = `
        from(bucket: "${bucket}")
          |> range(start: -2h)
          |> filter(fn: (r) => r._measurement == "weather")
          |> filter(fn: (r) => r._field == "temperature")
          |> group(columns: ["city_name"])
          |> last()
          |> group()
      `;

      const readings: Array<{ city: string; temp: number }> = [];
      const conditions = new Map<string, number>();

      await new Promise<void>((resolve, reject) => {
        queryApi.queryRows(statFlux, {
          next(row, tableMeta) {
            const obj = tableMeta.toObject(row);
            const temp = obj._value as number;
            const city = obj.city_name as string;
            const cond = (obj.weather_condition as string) ?? "unknown";

            readings.push({ city, temp });
            conditions.set(cond, (conditions.get(cond) ?? 0) + 1);
          },
          error: reject,
          complete: resolve,
        });
      });

      // Find most common condition
      let topCondition = "unknown";
      let topCount = 0;
      for (const [cond, count] of conditions) {
        if (count > topCount) { topCondition = cond; topCount = count; }
      }

      // Hottest and coldest city
      let hottestCity = { name: "—", temp: -Infinity };
      let coldestCity = { name: "—", temp: Infinity };
      for (const { city, temp } of readings) {
        if (temp > hottestCity.temp) hottestCity = { name: city, temp };
        if (temp < coldestCity.temp) coldestCity = { name: city, temp };
      }

      res.json({
        totalCities:  new Set(readings.map(r => r.city)).size,
        hottestCity:  readings.length > 0 ? { name: hottestCity.name, temp: Math.round(hottestCity.temp * 10) / 10 } : null,
        coldestCity:  readings.length > 0 ? { name: coldestCity.name, temp: Math.round(coldestCity.temp * 10) / 10 } : null,
        topCondition: topCondition,
        totalReadings: readings.length,
        timestamp:    new Date().toISOString(),
      });
    } catch (err: any) {
      console.error("[weather] summary error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/weather/:city/history?range=6h
  router.get("/:city/history", async (req: Request, res: Response) => {
    try {
      const rawCity = req.params.city;
      // Sanitize city name before injecting into Flux query
      const city = rawCity.replace(/[^a-zA-Z0-9 _\-\.]/g, "");
      const range = req.query.range as string ?? "1h";

      // Validate range — allow: 1h, 6h, 12h, 1d, 3d, 7d
      const validRanges = ["1h", "6h", "12h", "1d", "3d", "7d"];
      const safeRange = validRanges.includes(range) ? range : "1h";

      const flux = `
        from(bucket: "${bucket}")
          |> range(start: -${safeRange})
          |> filter(fn: (r) => r._measurement == "weather")
          |> filter(fn: (r) => r._field == "temperature")
          |> filter(fn: (r) => r.city_name == "${city}")
          |> sort(columns: ["_time"])
      `;

      const rows: any[] = [];
      await new Promise<void>((resolve, reject) => {
        queryApi.queryRows(flux, {
          next(row, tableMeta) {
            const obj = tableMeta.toObject(row);
            rows.push({
              temperature: obj._value ?? 0,
              recorded_at: obj._time  ?? null,
              weather_condition: obj.weather_condition ?? "unknown",
            });
          },
          error: reject,
          complete: resolve,
        });
      });

      res.json({
        city,
        range: safeRange,
        points: rows,
      });
    } catch (err: any) {
      console.error("[weather] history error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
