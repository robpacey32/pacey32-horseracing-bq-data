import {
  ANALYTICS_DATASET,
  getBigQueryClient,
  PROJECT_ID,
  RACE_DATASET,
} from "@/lib/bigquery";

// ============================================================
// TYPES & INTERFACES
// ============================================================

export interface RaceOption {
  date: string;
  location: string;
  time: string;
  status: string | null;
  raceKey: string;
  raceName: string | null;
}

export interface RaceRunner {
  // Pre-race information
  horseNumber: string;
  stallNumber: string | null;
  horseName: string;
  silkUrl: string | null;
  headgear: string | null;
  lastRun: string | null;
  commentary: string | null;
  age: string | null;
  weight: string | null;
  jockey: string | null;
  trainer: string | null;
  odds: string | null;
  form: string | null;
  raceHistoryStats: string | null;

  // Post-race information
  postHorseNumber: string | null;
  postStallNumber: string | null;
  postHorseName: string | null;
  position: string | null;
  result: string | null;
  sp: string | null;
  postJockey: string | null;
  postTrainer: string | null;
  prizeMoney: string | null;
  rideDescription: string | null;
}

export interface RaceDetail {
  date: string;
  dayOfWeek: string | null;
  location: string;
  time: string;
  name: string;
  raceClass: string | null;
  distance: string | null;
  going: string | null;
  runners: string | null;
  surface: string | null;
  sourceUrl: string | null;

  winningTime: string | null;
  offTime: string | null;
  postWinningTime: string | null;
  postGoing: string | null;

  entries: RaceRunner[];
}

export type Last12MonthEntityType =
  | "HORSE"
  | "JOCKEY"
  | "TRAINER";

// Individual historical race for Last 12 Months drill-down
export interface Last12MonthRace {
  raceId: string | null;
  date: string;
  location: string;
  time: string;
  raceName: string | null;
  position: string | null;
  positionInt: number | null;
  placed: boolean;
  won: boolean;
}

// Aggregated statistics plus underlying races
export interface Last12MonthStat {
  entity: string;
  races: number;
  wins: number;
  places: number;
  prizeMoneyTotal: number;
  raceDetails: Last12MonthRace[];
}

// ============================================================
// RACE DATES
// ============================================================

export async function getRaceDates(): Promise<string[]> {
  const query = `
    SELECT DISTINCT
      Date AS race_date

    FROM \`${PROJECT_ID}.${RACE_DATASET}.RaceSpine_Latest\`

    WHERE Date IS NOT NULL

    ORDER BY race_date DESC
  `;

  const [rows] = await getBigQueryClient().query({
    query,
  });

  return rows.map(
    (row) => row.race_date as string
  );
}

// ============================================================
// RACES FOR SELECTED DATE
// ============================================================

export async function getRacesForDate(
  date: string
): Promise<RaceOption[]> {
  const query = `
    WITH race_names AS (
      SELECT
        Pre_RaceDate,
        Pre_RaceLocation,
        Pre_RaceTime,
        ANY_VALUE(Pre_RaceName) AS race_name

      FROM \`${PROJECT_ID}.${RACE_DATASET}.RaceFull_Latest\`

      WHERE Pre_RaceName IS NOT NULL

      GROUP BY
        Pre_RaceDate,
        Pre_RaceLocation,
        Pre_RaceTime
    )

    SELECT
      s.Date AS race_date,
      s.Location,
      s.Time,
      s.Status,
      s.race_key,
      r.race_name

    FROM \`${PROJECT_ID}.${RACE_DATASET}.RaceSpine_Latest\` s

    LEFT JOIN race_names r
      ON r.Pre_RaceDate = FORMAT_DATE(
        '%d/%m/%Y',
        SAFE.PARSE_DATE('%Y-%m-%d', s.Date)
      )
      AND r.Pre_RaceLocation = s.Location
      AND r.Pre_RaceTime = s.Time

    WHERE s.Date = @date

    ORDER BY
      s.Location,
      s.Time
  `;

  const [rows] = await getBigQueryClient().query({
    query,
    params: {
      date,
    },
  });

  return rows.map((row) => ({
    date: row.race_date as string,
    location: row.Location as string,
    time: row.Time as string,
    status: (row.Status as string | null) ?? null,
    raceKey: row.race_key as string,
    raceName: (row.race_name as string | null) ?? null,
  }));
}

// ============================================================
// DATE FORMATTING
// ============================================================

// Converts YYYY-MM-DD to DD/MM/YYYY for RaceFull_Latest
function toRaceFullDate(date: string): string {
  const [year, month, day] = date.split("-");

  return `${day}/${month}/${year}`;
}

// ============================================================
// SELECTED RACE DETAIL
// ============================================================

export async function getRaceDetail(
  date: string,
  location: string,
  time: string
): Promise<RaceDetail | null> {
  const query = `
    SELECT
      -- Pre-race runner information
      HorseNumber,
      StallNumber,
      HorseName,
      SilkURL,
      Headgear,
      LastRun,
      Commentary,
      Age,
      Weight,
      Jockey,
      Trainer,
      Odds,
      Form,
      RaceHistoryStats,

      -- Pre-race information
      Pre_RaceDate,
      Pre_RaceDayOfWeek,
      Pre_RaceLocation,
      Pre_RaceName,
      Pre_RaceTime,
      Pre_RaceClass,
      Pre_RaceDistance,
      Pre_RaceGoing,
      Pre_RaceRunners,
      Pre_RaceSurface,
      Pre_WinningTime,
      Pre_OffTime,
      Pre_SourceURL,

      -- Post-race runner information
      Post_HorseNumber,
      Post_StallNumber,
      Post_HorseName,
      Pos,
      Result,
      SP,
      Post_Trainer,
      Post_Jockey,
      PrizeMoney,
      RideDescription,

      -- Post-race information
      Post_WinningTime,
      Post_RaceGoing

    FROM \`${PROJECT_ID}.${RACE_DATASET}.RaceFull_Latest\`

    WHERE Pre_RaceDate = @date
      AND Pre_RaceLocation = @location
      AND Pre_RaceTime = @time

    ORDER BY SAFE_CAST(HorseNumber AS INT64)
  `;

  const [rows] = await getBigQueryClient().query({
    query,

    params: {
      date: toRaceFullDate(date),
      location,
      time,
    },
  });

  if (!rows.length) {
    return null;
  }

  const first = rows[0];

  return {
    // Race identity
    date: first.Pre_RaceDate as string,

    dayOfWeek:
      (first.Pre_RaceDayOfWeek as string | null) ?? null,

    location: first.Pre_RaceLocation as string,

    time: first.Pre_RaceTime as string,

    name: first.Pre_RaceName as string,

    // Race characteristics
    raceClass:
      (first.Pre_RaceClass as string | null) ?? null,

    distance:
      (first.Pre_RaceDistance as string | null) ?? null,

    going:
      (first.Pre_RaceGoing as string | null) ?? null,

    runners:
      (first.Pre_RaceRunners as string | null) ?? null,

    surface:
      (first.Pre_RaceSurface as string | null) ?? null,

    sourceUrl:
      (first.Pre_SourceURL as string | null) ?? null,

    // Timing and conditions
    winningTime:
      (first.Pre_WinningTime as string | null) ?? null,

    offTime:
      (first.Pre_OffTime as string | null) ?? null,

    postWinningTime:
      (first.Post_WinningTime as string | null) ?? null,

    postGoing:
      (first.Post_RaceGoing as string | null) ?? null,

    // Runners
    entries: rows.map((row) => ({
      // Pre-race runner details
      horseNumber: row.HorseNumber as string,

      stallNumber:
        (row.StallNumber as string | null) ?? null,

      horseName: row.HorseName as string,

      silkUrl:
        (row.SilkURL as string | null) ?? null,

      headgear:
        (row.Headgear as string | null) ?? null,

      lastRun:
        (row.LastRun as string | null) ?? null,

      commentary:
        (row.Commentary as string | null) ?? null,

      age:
        (row.Age as string | null) ?? null,

      weight:
        (row.Weight as string | null) ?? null,

      jockey:
        (row.Jockey as string | null) ?? null,

      trainer:
        (row.Trainer as string | null) ?? null,

      odds:
        (row.Odds as string | null) ?? null,

      form:
        (row.Form as string | null) ?? null,

      raceHistoryStats:
        (row.RaceHistoryStats as string | null) ?? null,

      // Post-race runner details
      postHorseNumber:
        (row.Post_HorseNumber as string | null) ?? null,

      postStallNumber:
        (row.Post_StallNumber as string | null)
          ?.replace(/[()]/g, "") ?? null,

      postHorseName:
        (row.Post_HorseName as string | null) ?? null,

      position:
        (row.Pos as string | null) ?? null,

      result:
        (row.Result as string | null) ?? null,

      sp:
        (row.SP as string | null) ?? null,

      postJockey:
        (row.Post_Jockey as string | null) ?? null,

      postTrainer:
        (row.Post_Trainer as string | null) ?? null,

      prizeMoney:
        (row.PrizeMoney as string | null) ?? null,

      rideDescription:
        (row.RideDescription as string | null) ?? null,
    })),
  };
}

// ============================================================
// LAST 12 MONTHS STATISTICS
// ============================================================

export async function getLast12MonthStats(
  names: string[],
  asOfDate: string,
  entityType: Last12MonthEntityType,
  eurToGbp: number
): Promise<Last12MonthStat[]> {
  if (!names.length) {
    return [];
  }

  const query = `
    -- Identify the relevant horse, jockey or trainer
    -- and collect races within the selected 12-month period.

    WITH race_data AS (
      SELECT
        CASE
          WHEN @entity_type = 'HORSE' THEN HorseName
          WHEN @entity_type = 'JOCKEY' THEN Jockey
          WHEN @entity_type = 'TRAINER' THEN Trainer
          ELSE NULL
        END AS Entity,

        RaceId,
        RaceDate,
        RaceLocation,
        RaceTime,
        RaceName,
        Pos,
        PosInt,
        Placed,
        PrizeMoney,
        PrizeMoneyNumeric

      FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.RaceTotalsForApp\`

      WHERE RaceDate BETWEEN
        DATE_SUB(
          CAST(@as_of_date AS DATE),
          INTERVAL 12 MONTH
        )
        AND CAST(@as_of_date AS DATE)
    ),

    -- Restrict results to entities in the selected race.

    filtered AS (
      SELECT *
      FROM race_data

      WHERE Entity IN UNNEST(@names)
        AND Entity IS NOT NULL
    )

    SELECT
      Entity,

      -- Aggregated performance
      COUNT(*) AS Races,

      COUNTIF(PosInt = 1) AS Wins,

      SUM(
        COALESCE(Placed, 0)
      ) AS Places,

      SUM(
        CASE
          WHEN STRPOS(PrizeMoney, '£') > 0
            THEN COALESCE(PrizeMoneyNumeric, 0)

          WHEN STRPOS(PrizeMoney, '€') > 0
            THEN COALESCE(PrizeMoneyNumeric, 0) * @eur_to_gbp

          ELSE 0
        END
      ) AS PrizeMoneyTotal,

      -- Individual races for drill-down.
      -- Most recent races appear first.

      ARRAY_AGG(
        STRUCT(
          RaceId AS raceId,

          CAST(RaceDate AS STRING) AS date,

          RaceLocation AS location,

          RaceTime AS time,

          RaceName AS raceName,

          Pos AS position,

          PosInt AS positionInt,

          COALESCE(Placed, 0) = 1 AS placed,

          PosInt = 1 AS won
        )

        ORDER BY
          RaceDate DESC,
          RaceTime DESC,
          RaceId DESC
      ) AS raceDetails

    FROM filtered

    GROUP BY Entity

    ORDER BY
      PrizeMoneyTotal DESC,
      Wins DESC,
      Places DESC,
      Races DESC
  `;

  const [rows] = await getBigQueryClient().query({
    query,

    params: {
      names,
      as_of_date: asOfDate,
      entity_type: entityType,
      eur_to_gbp: eurToGbp,
    },

    types: {
      names: ["STRING"],
      as_of_date: "STRING",
      entity_type: "STRING",
      eur_to_gbp: "FLOAT64",
    },
  });

  return rows.map((row) => ({
    entity: String(row.Entity ?? ""),

    races: Number(row.Races ?? 0),

    wins: Number(row.Wins ?? 0),

    places: Number(row.Places ?? 0),

    prizeMoneyTotal: Number(
      row.PrizeMoneyTotal ?? 0
    ),

    raceDetails: (
      row.raceDetails ?? []
    ).map(
      (race: {
        raceId: string | null;
        date: string;
        location: string;
        time: string;
        raceName: string | null;
        position: string | null;
        positionInt: number | null;
        placed: boolean;
        won: boolean;
      }) => ({
        raceId: race.raceId ?? null,

        date: race.date,

        location: race.location,

        time: race.time,

        raceName: race.raceName ?? null,

        position: race.position ?? null,

        positionInt:
          race.positionInt === null
            ? null
            : Number(race.positionInt),

        placed: Boolean(race.placed),

        won: Boolean(race.won),
      })
    ),
  }));
}