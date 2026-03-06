import type { ChartConfiguration, ChartType } from "chart.js";
import { ChartBuildResult, ChartBuildRequest, ChartTool } from "../../core/interfaces.js";
import { QueryResult } from "../../core/types.js";

function asFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function pickFirstNumericColumn(result: QueryResult): string | null {
  for (const column of result.columns) {
    for (const row of result.rows) {
      if (asFiniteNumber(row[column]) !== null) {
        return column;
      }
    }
  }
  return null;
}

function pickFirstTextColumn(result: QueryResult, exclude: Set<string>): string | null {
  for (const column of result.columns) {
    if (exclude.has(column)) {
      continue;
    }
    for (const row of result.rows) {
      const value = row[column];
      if (value !== null && value !== undefined) {
        return column;
      }
    }
  }
  return null;
}

function toLabel(value: unknown): string {
  if (value === null || value === undefined) {
    return "(null)";
  }
  return String(value);
}

export class ChartJsTool implements ChartTool {
  buildFromQueryResult(input: {
    request: ChartBuildRequest;
    result: QueryResult;
    maxPoints: number;
  }): ChartBuildResult {
    const requestedType = input.request.type ?? "bar";
    const type: ChartType =
      requestedType === "line" || requestedType === "pie" || requestedType === "doughnut" ? requestedType : "bar";
    const limit = Math.max(1, Math.min(input.request.maxPoints ?? input.maxPoints, input.maxPoints));
    const rows = input.result.rows.slice(0, limit);
    const yKey = input.request.yKey ?? pickFirstNumericColumn(input.result);
    const xKey =
      input.request.xKey ??
      pickFirstTextColumn(input.result, new Set(yKey ? [yKey] : [])) ??
      input.result.columns[0] ??
      null;
    const seriesKey = input.request.seriesKey ?? null;

    if (!xKey) {
      throw new Error("Could not infer chart xKey from query result. Provide chartRequest.xKey.");
    }
    if (!yKey) {
      throw new Error("Could not infer chart yKey from query result. Provide chartRequest.yKey.");
    }

    const labels: string[] = [];
    const labelsSeen = new Set<string>();
    const datasetMap = new Map<string, Map<string, number>>();

    for (const row of rows) {
      const label = toLabel(row[xKey]);
      if (!labelsSeen.has(label)) {
        labelsSeen.add(label);
        labels.push(label);
      }
      const seriesName = seriesKey ? toLabel(row[seriesKey]) : (input.request.title ?? yKey);
      const value = asFiniteNumber(row[yKey]);
      if (value === null) {
        continue;
      }
      if (!datasetMap.has(seriesName)) {
        datasetMap.set(seriesName, new Map<string, number>());
      }
      const pointMap = datasetMap.get(seriesName) as Map<string, number>;
      pointMap.set(label, (pointMap.get(label) ?? 0) + value);
    }

    const datasets = Array.from(datasetMap.entries()).map(([name, pointMap]) => ({
      label: name,
      data: labels.map((label) => pointMap.get(label) ?? 0)
    }));

    const config: ChartConfiguration = {
      type,
      data: {
        labels,
        datasets
      },
      options: {
        responsive: false,
        plugins: {
          title: input.request.title
            ? {
                display: true,
                text: input.request.title
              }
            : undefined
        }
      }
    };

    return {
      config: config as unknown as Record<string, unknown>,
      summary: {
        type,
        xKey,
        yKey,
        seriesKey,
        labelsCount: labels.length,
        datasetsCount: datasets.length,
        pointsCount: labels.length * Math.max(1, datasets.length)
      }
    };
  }
}
