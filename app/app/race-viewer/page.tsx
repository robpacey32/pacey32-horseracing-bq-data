"use client";

import {
  Fragment,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

interface RaceOption {
  date: string;
  location: string;
  time: string;
  status: string | null;
  raceKey: string;
  raceName: string | null;
}

interface RaceRunner {
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

interface RaceDetail {
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

interface Last12MonthRace {
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

interface Last12MonthStat {
  raceDetails: Last12MonthRace[];
  entity: string;
  races: number;
  wins: number;
  places: number;
  prizeMoneyTotal: number;
}

type RaceTab =
  | "pre-race"
  | "results"
  | "last-12-months";

type Last12MonthEntityType =
  | "HORSE"
  | "JOCKEY"
  | "TRAINER";

type Currency =
  | "GBP"
  | "EUR";

type PreRaceSortField =
  | "oddsRank"
  | "horseNumber"
  | "stallNumber"
  | "horseName"
  | "age"
  | "weight"
  | "jockey"
  | "trainer"
  | "odds";

type ResultSortField =
  | "position"
  | "horseNumber"
  | "stallNumber"
  | "horseName"
  | "sp"
  | "prizeMoney"
  | "jockey"
  | "trainer";

type Last12SortField =
  | "entity" | "races" | "wins" | "winPct" | "places"
  | "placePct" | "combinedPct" | "prizeMoneyTotal";

type HistoryCategory = "wins" | "places" | "combined";

interface HistorySelection {
  entity: string;
  category: HistoryCategory;
  races: Last12MonthRace[];
}

type SortDirection =
  | "asc"
  | "desc";

interface ExchangeRate {
  gbpToEur: number;
  eurToGbp: number;
  date: string;
  source: "ECB";
}

function formatDisplayDate(
  date: string
) {
  if (!date) {
    return "";
  }

  const parts =
    date.split("-");

  if (parts.length !== 3) {
    return date;
  }

  const [year, month, day] =
    parts.map(Number);

  if (
    !year ||
    !month ||
    !day
  ) {
    return date;
  }

  return new Intl.DateTimeFormat(
    "en-GB",
    {
      weekday: "long",
      day: "numeric",
      month: "long",
      year: "numeric",
      timeZone: "UTC",
    }
  ).format(
    new Date(
      Date.UTC(
        year,
        month - 1,
        day
      )
    )
  );
}

function formatRaceDate(
  date: string,
  dayOfWeek: string | null
) {
  if (!date) {
    return dayOfWeek ?? "";
  }

  const parts =
    date.split("/");

  if (parts.length !== 3) {
    return dayOfWeek
      ? `${dayOfWeek}, ${date}`
      : date;
  }

  const [day, month, year] =
    parts.map(Number);

  if (
    !day ||
    !month ||
    !year
  ) {
    return dayOfWeek
      ? `${dayOfWeek}, ${date}`
      : date;
  }

  const formatted =
    new Intl.DateTimeFormat(
      "en-GB",
      {
        day: "numeric",
        month: "long",
        year: "numeric",
        timeZone: "UTC",
      }
    ).format(
      new Date(
        Date.UTC(
          year,
          month - 1,
          day
        )
      )
    );

  return dayOfWeek
    ? `${dayOfWeek}, ${formatted}`
    : formatted;
}

function cleanRaceHistory(
  value: string | null
) {
  if (!value) {
    return "";
  }

  return value
    .split("|")
    .map((item) =>
      item.trim()
    )
    .filter(Boolean)
    .join(" · ");
}

function oddsToNumber(
  odds: string | null
) {
  if (!odds) {
    return Number.POSITIVE_INFINITY;
  }

  const cleaned = odds
    .trim()
    .toLowerCase()
    .replace("evens", "1/1")
    .replace("even", "1/1")
    .replace("evs", "1/1");

  if (
    cleaned.includes("/")
  ) {
    const [
      numerator,
      denominator,
    ] =
      cleaned.split("/");

    const num =
      Number(numerator);

    const den =
      Number(denominator);

    if (
      Number.isFinite(num) &&
      Number.isFinite(den) &&
      den !== 0
    ) {
      return num / den;
    }
  }

  const numeric =
    Number(cleaned);

  return Number.isFinite(
    numeric
  )
    ? numeric
    : Number.POSITIVE_INFINITY;
}

function positionSortValue(
  position: string | null
) {
  if (!position) {
    return 999;
  }

  const cleaned =
    position
      .toLowerCase()
      .trim()
      .replace(
        /(st|nd|rd|th)$/,
        ""
      );

  const numeric =
    Number(cleaned);

  return Number.isFinite(
    numeric
  )
    ? numeric
    : 900;
}

function SortableHeader({
  label,
  field,
  activeField,
  direction,
  onSort,
  className = "",
}: {
  label: string;
  field: PreRaceSortField;
  activeField: PreRaceSortField;
  direction: SortDirection;
  onSort: (
    field: PreRaceSortField
  ) => void;
  className?: string;
}) {
  const active =
    activeField === field;

  return (
    <th
      className={`racing-small-caps px-2 py-2.5 text-[9px] font-semibold text-[var(--racing-muted)] ${className}`}
    >
      <button
        type="button"
        onClick={() =>
          onSort(field)
        }
        className="flex w-full items-center gap-1 text-left hover:text-[var(--racing-green)]"
        title={`Sort by ${label}`}
      >
        <span>
          {label}
        </span>

        <span
          className={`text-[8px] ${
            active
              ? "text-[var(--racing-gold-dark)]"
              : "text-[var(--racing-border)]"
          }`}
        >
          {active
            ? direction ===
              "asc"
              ? "▲"
              : "▼"
            : "↕"}
        </span>
      </button>
    </th>
  );
}

function ResultSortableHeader({
  label,
  field,
  activeField,
  direction,
  onSort,
  className = "",
}: {
  label: string;
  field: ResultSortField;
  activeField: ResultSortField;
  direction: SortDirection;
  onSort: (field: ResultSortField) => void;
  className?: string;
}) {
  const active = activeField === field;

  return (
    <th className={`racing-small-caps px-2 py-2.5 text-[9px] font-semibold text-[var(--racing-muted)] ${className}`}>
      <button
        type="button"
        onClick={() => onSort(field)}
        className="flex w-full items-center gap-1 text-left hover:text-[var(--racing-green)]"
        title={`Sort by ${label}`}
      >
        <span>{label}</span>
        <span className={`text-[8px] ${active ? "text-[var(--racing-gold-dark)]" : "text-[var(--racing-border)]"}`}>
          {active ? (direction === "asc" ? "▲" : "▼") : "↕"}
        </span>
      </button>
    </th>
  );
}

function Last12SortableHeader({
  label, field, activeField, direction, onSort, className = "",
}: {
  label: string;
  field: Last12SortField;
  activeField: Last12SortField;
  direction: SortDirection;
  onSort: (field: Last12SortField) => void;
  className?: string;
}) {
  const active = activeField === field;
  return (
    <th className={`racing-small-caps px-2 py-2.5 text-left text-[9px] font-semibold text-[var(--racing-muted)] ${className}`}>
      <button type="button" onClick={() => onSort(field)}
        className="flex w-full items-center gap-1 text-left hover:text-[var(--racing-green)]">
        {label}
        <span className={`text-[8px] ${active ? "text-[var(--racing-gold-dark)]" : "text-[var(--racing-border)]"}`}>
          {active ? (direction === "asc" ? "▲" : "▼") : "↕"}
        </span>
      </button>
    </th>
  );
}

export default function RaceViewerPage() {
  const [
    dates,
    setDates,
  ] =
    useState<string[]>(
      []
    );

  const [
    races,
    setRaces,
  ] =
    useState<
      RaceOption[]
    >([]);

  const [
    selectedDate,
    setSelectedDate,
  ] = useState("");

  const [
    selectedLocation,
    setSelectedLocation,
  ] = useState("");

  const [
    selectedTime,
    setSelectedTime,
  ] = useState("");

  const [
    race,
    setRace,
  ] =
    useState<RaceDetail | null>(
      null
    );

  const [
    activeTab,
    setActiveTab,
  ] =
    useState<RaceTab>(
      "pre-race"
    );

  const [
    expandedMeetings,
    setExpandedMeetings,
  ] = useState<
    Set<string>
  >(new Set());

  const [
    expandedHorse,
    setExpandedHorse,
  ] =
    useState<string | null>(
      null
    );

  const [
    preRaceSortField,
    setPreRaceSortField,
  ] =
    useState<PreRaceSortField>(
      "oddsRank"
    );

  const [
    preRaceSortDirection,
    setPreRaceSortDirection,
  ] =
    useState<SortDirection>(
      "asc"
    );

  const [
    resultSortField,
    setResultSortField,
  ] = useState<ResultSortField>("position");

  const [
    resultSortDirection,
    setResultSortDirection,
  ] = useState<SortDirection>("asc");

  const [
    expandedResultHorse,
    setExpandedResultHorse,
  ] = useState<string | null>(null);

  const [
    last12MonthEntityType,
    setLast12MonthEntityType,
  ] =
    useState<Last12MonthEntityType>(
      "HORSE"
    );

  const [
    currency,
    setCurrency,
  ] =
    useState<Currency>(
      "GBP"
    );

  const [exchangeRate, setExchangeRate] = useState<ExchangeRate | null>(null);

  const [
    last12MonthStats,
    setLast12MonthStats,
  ] =
    useState<
      Last12MonthStat[]
    >([]);

  const [
    loadingRaces,
    setLoadingRaces,
  ] =
    useState(true);

  const [
    loadingRace,
    setLoadingRace,
  ] =
    useState(false);

  const [
    loadingLast12Months,
    setLoadingLast12Months,
  ] =
    useState(false);

  const [
    error,
    setError,
  ] = useState("");

  const [
    last12MonthError,
    setLast12MonthError,
  ] = useState("");

  const [last12SortField, setLast12SortField] = useState<Last12SortField>("prizeMoneyTotal");
  const [last12SortDirection, setLast12SortDirection] = useState<SortDirection>("desc");
  const [historySelection, setHistorySelection] = useState<HistorySelection | null>(null);
  const [showAllHistory, setShowAllHistory] = useState(false);
  
  const [calendarOpen, setCalendarOpen] = useState(false);
  const [calendarMonth, setCalendarMonth] = useState<Date | null>(null);
  const [todayDate, setTodayDate] = useState<string | null>(null);

  const calendarRef = useRef<HTMLDivElement | null>(null);
  const availableDates = useMemo(() => new Set(dates), [dates]);

  useEffect(() => {
    setTodayDate(localDateString());
  }, []);

  const localDateString = (offsetDays = 0) => {
    const date = new Date();
    date.setDate(date.getDate() + offsetDays);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  };

  function chooseCalendarDate(date: string) {
    if (!availableDates.has(date)) return;
    setCalendarOpen(false);
    setHistoryNavigationError("");
    pendingHistoricalRace.current = null;
    setSelectedDate(date);
  }

  useEffect(() => {
    if (!calendarOpen) return;
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (calendarRef.current && !calendarRef.current.contains(event.target as Node)) {
        setCalendarOpen(false);
      }
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setCalendarOpen(false);
    };
    document.addEventListener("mousedown", closeOnOutsideClick);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("mousedown", closeOnOutsideClick);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, [calendarOpen]);
  const [historyNavigationError, setHistoryNavigationError] = useState("");
  const pendingHistoricalRace = useRef<Last12MonthRace | null>(null);

  // Load dates
  useEffect(() => {
    async function loadDates() {
      try {
        setLoadingRaces(
          true
        );

        const response =
          await fetch(
            "/api/race-viewer/races"
          );

        if (!response.ok) {
          throw new Error();
        }

        const data =
          await response.json();

        const loadedDates:
          string[] =
          data.dates ?? [];

        setDates(
          loadedDates
        );

        if (
          loadedDates.length >
          0
        ) {
          setSelectedDate(
            loadedDates[0]
          );
        }
      } catch {
        setError(
          "Unable to load race dates."
        );
      } finally {
        setLoadingRaces(
          false
        );
      }
    }

    loadDates();
  }, []);

  // Load races for selected date
  useEffect(() => {
    if (!selectedDate) {
      return;
    }

    async function loadRaces() {
      try {
        setLoadingRaces(
          true
        );

        setError("");
        setRace(null);
        setExpandedHorse(
          null
        );

        const response =
          await fetch(
            `/api/race-viewer/races?date=${encodeURIComponent(
              selectedDate
            )}`
          );

        if (!response.ok) {
          throw new Error();
        }

        const data =
          await response.json();

        const loadedRaces:
          RaceOption[] =
          data.races ?? [];

        setRaces(
          loadedRaces
        );

        const requested = pendingHistoricalRace.current;
        const historicalMatch = requested?.date === selectedDate
          ? loadedRaces.find((item) =>
              item.location.trim().toLowerCase() === requested.location.trim().toLowerCase() &&
              item.time.trim() === requested.time.trim()
            )
          : undefined;
        const chosenRace = historicalMatch ?? (requested ? undefined : loadedRaces[0]);

        if (chosenRace) {
          setSelectedLocation(chosenRace.location);
          setSelectedTime(chosenRace.time);
          setExpandedMeetings(new Set([chosenRace.location]));
          setActiveTab(historicalMatch ? "results" : "pre-race");
        } else {
          setSelectedLocation("");
          setSelectedTime("");
          setExpandedMeetings(new Set());
          if (requested) {
            setHistoryNavigationError("That historical race is not available in the Race Viewer race spine.");
          }
        }
        pendingHistoricalRace.current = null;
      } catch {
        setError(
          "Unable to load races."
        );
      } finally {
        setLoadingRaces(
          false
        );
      }
    }

    loadRaces();
  }, [selectedDate]);

  const locations =
    useMemo(
      () => [
        ...new Set(
          races.map(
            (item) =>
              item.location
          )
        ),
      ],
      [races]
    );

  const racesByLocation =
    useMemo(() => {
      const grouped =
        new Map<
          string,
          RaceOption[]
        >();

      for (
        const location
        of locations
      ) {
        grouped.set(
          location,
          races.filter(
            (item) =>
              item.location ===
              location
          )
        );
      }

      return grouped;
    }, [
      locations,
      races,
    ]);

  // Load selected race
  useEffect(() => {
    if (
      !selectedDate ||
      !selectedLocation ||
      !selectedTime
    ) {
      return;
    }

    async function loadRace() {
      try {
        setLoadingRace(
          true
        );

        setError("");
        setRace(null);
        setExpandedHorse(
          null
        );

        setLast12MonthStats(
          []
        );
        setExchangeRate(null);

        const params =
          new URLSearchParams(
            {
              date:
                selectedDate,
              location:
                selectedLocation,
              time:
                selectedTime,
            }
          );

        const response =
          await fetch(
            `/api/race-viewer/detail?${params.toString()}`
          );

        if (!response.ok) {
          throw new Error();
        }

        const data =
          await response.json();

        setRace(
          data.race ??
            null
        );
      } catch {
        setError(
          "Unable to load selected race."
        );
      } finally {
        setLoadingRace(
          false
        );
      }
    }

    loadRace();
  }, [
    selectedDate,
    selectedLocation,
    selectedTime,
  ]);

  // Pre-race order and dense odds rank
  const preRaceEntries =
    useMemo(() => {
      if (!race) {
        return [];
      }

      const sorted = [
        ...race.entries,
      ].sort((a, b) => {
        const oddsDifference =
          oddsToNumber(
            a.odds
          ) -
          oddsToNumber(
            b.odds
          );

        if (
          oddsDifference !==
          0
        ) {
          return oddsDifference;
        }

        return (
          Number(
            a.horseNumber
          ) -
          Number(
            b.horseNumber
          )
        );
      });

      let previousOdds:
        number | null =
        null;

      let rank = 0;

      return sorted.map(
        (runner) => {
          const odds =
            oddsToNumber(
              runner.odds
            );

          if (
            previousOdds ===
              null ||
            odds !==
              previousOdds
          ) {
            rank += 1;
          }

          previousOdds =
            odds;

          return {
            ...runner,

            oddsRank:
              Number.isFinite(
                odds
              )
                ? rank
                : null,
          };
        }
      );
    }, [race]);

    const sortedPreRaceEntries =
      useMemo(() => {
        const sorted = [
          ...preRaceEntries,
        ];

        function compareText(
          a: string | null,
          b: string | null
        ) {
          const aValue =
            a?.trim() ?? "";

          const bValue =
            b?.trim() ?? "";

          if (
            !aValue &&
            !bValue
          ) {
            return 0;
          }

          if (!aValue) {
            return 1;
          }

          if (!bValue) {
            return -1;
          }

          return aValue.localeCompare(
            bValue,
            "en-GB",
            {
              sensitivity:
                "base",
            }
          );
        }

        function compareNumber(
          a:
            | string
            | number
            | null
            | undefined,
          b:
            | string
            | number
            | null
            | undefined
        ) {
          const aNumber =
            Number(a);

          const bNumber =
            Number(b);

          const aValid =
            a !== null &&
            a !== undefined &&
            String(a).trim() !==
              "" &&
            Number.isFinite(
              aNumber
            );

          const bValid =
            b !== null &&
            b !== undefined &&
            String(b).trim() !==
              "" &&
            Number.isFinite(
              bNumber
            );

          if (
            !aValid &&
            !bValid
          ) {
            return 0;
          }

          if (!aValid) {
            return 1;
          }

          if (!bValid) {
            return -1;
          }

          return (
            aNumber -
            bNumber
          );
        }

        function weightToNumber(
          weight:
            | string
            | null
        ) {
          if (!weight) {
            return null;
          }

          const cleaned =
            weight
              .toLowerCase()
              .trim();

          const stonePounds =
            cleaned.match(
              /(\d+)\s*-\s*(\d+)/
            );

          if (stonePounds) {
            return (
              Number(
                stonePounds[1]
              ) *
                14 +
              Number(
                stonePounds[2]
              )
            );
          }

          const numeric =
            Number(cleaned);

          return Number.isFinite(
            numeric
          )
            ? numeric
            : null;
        }

        sorted.sort(
          (a, b) => {
            let comparison =
              0;

            switch (
              preRaceSortField
            ) {
              case "oddsRank":
                comparison =
                  compareNumber(
                    a.oddsRank,
                    b.oddsRank
                  );
                break;

              case "horseNumber":
                comparison =
                  compareNumber(
                    a.horseNumber,
                    b.horseNumber
                  );
                break;

              case "stallNumber":
                comparison =
                  compareNumber(
                    a.stallNumber,
                    b.stallNumber
                  );
                break;

              case "horseName":
                comparison =
                  compareText(
                    a.horseName,
                    b.horseName
                  );
                break;

              case "age":
                comparison =
                  compareNumber(
                    a.age,
                    b.age
                  );
                break;

              case "weight":
                comparison =
                  compareNumber(
                    weightToNumber(
                      a.weight
                    ),
                    weightToNumber(
                      b.weight
                    )
                  );
                break;

              case "jockey":
                comparison =
                  compareText(
                    a.jockey,
                    b.jockey
                  );
                break;

              case "trainer":
                comparison =
                  compareText(
                    a.trainer,
                    b.trainer
                  );
                break;

              case "odds":
                comparison =
                  compareNumber(
                    oddsToNumber(
                      a.odds
                    ),
                    oddsToNumber(
                      b.odds
                    )
                  );
                break;
            }

            if (
              comparison === 0
            ) {
              comparison =
                compareNumber(
                  a.horseNumber,
                  b.horseNumber
                );
            }

            return preRaceSortDirection ===
              "asc"
              ? comparison
              : -comparison;
          }
        );

        return sorted;
      }, [
        preRaceEntries,
        preRaceSortField,
        preRaceSortDirection,
      ]);

  const showStallNumber =
    useMemo(
      () =>
        Boolean(
          race?.entries.some(
            (runner) =>
              runner.stallNumber !==
                null &&
              runner.stallNumber !==
                undefined &&
              !(["", "N/A", "NA", "-", "—"].includes(String(runner.stallNumber).trim().toUpperCase()))
          )
        ),
      [race]
    );

  // Results order and sorting
  const resultEntries = useMemo(() => {
    if (!race) return [];

    const rows = race.entries.filter(
      (runner) => runner.position || runner.result || runner.sp
    );

    const textValue = (value: string | null | undefined) =>
      (value ?? "").trim();

    const compareText = (a: string | null | undefined, b: string | null | undefined) =>
      textValue(a).localeCompare(textValue(b), "en-GB", { sensitivity: "base" });

    const numericValue = (value: string | null | undefined) => {
      if (!value) return Number.POSITIVE_INFINITY;
      const match = value.replace(/,/g, "").match(/-?\d+(?:\.\d+)?/);
      return match ? Number(match[0]) : Number.POSITIVE_INFINITY;
    };

    return [...rows].sort((a, b) => {
      let comparison = 0;

      switch (resultSortField) {
        case "position":
          comparison = positionSortValue(a.position || a.result) - positionSortValue(b.position || b.result);
          break;
        case "horseNumber":
          comparison = numericValue(a.postHorseNumber || a.horseNumber) - numericValue(b.postHorseNumber || b.horseNumber);
          break;
        case "stallNumber":
          comparison = numericValue(a.postStallNumber || a.stallNumber) - numericValue(b.postStallNumber || b.stallNumber);
          break;
        case "horseName":
          comparison = compareText(a.postHorseName || a.horseName, b.postHorseName || b.horseName);
          break;
        case "sp":
          comparison = oddsToNumber(a.sp) - oddsToNumber(b.sp);
          break;
        case "prizeMoney":
          comparison = numericValue(a.prizeMoney) - numericValue(b.prizeMoney);
          break;
        case "jockey":
          comparison = compareText(a.postJockey || a.jockey, b.postJockey || b.jockey);
          break;
        case "trainer":
          comparison = compareText(a.postTrainer || a.trainer, b.postTrainer || b.trainer);
          break;
      }

      if (comparison === 0) {
        comparison = positionSortValue(a.position || a.result) - positionSortValue(b.position || b.result);
      }

      return resultSortDirection === "asc" ? comparison : -comparison;
    });
  }, [race, resultSortField, resultSortDirection]);

  const hasResults = resultEntries.length > 0;

  const showResultStallNumber = useMemo(
    () => resultEntries.some((runner) => {
      const value = runner.postStallNumber || runner.stallNumber;
      return value !== null && value !== undefined && !["", "N/A", "NA", "-", "—"].includes(String(value).trim().toUpperCase());
    }),
    [resultEntries]
  );

  // Entities for Last 12 Months
  const last12MonthNames =
    useMemo(() => {
      if (!race) {
        return [];
      }

      let names:
        (string | null)[] =
        [];

      if (
        last12MonthEntityType ===
        "HORSE"
      ) {
        names =
          race.entries.map(
            (runner) =>
              runner.horseName
          );
      }

      if (
        last12MonthEntityType ===
        "JOCKEY"
      ) {
        names =
          race.entries.map(
            (runner) =>
              runner.jockey
          );
      }

      if (
        last12MonthEntityType ===
        "TRAINER"
      ) {
        names =
          race.entries.map(
            (runner) =>
              runner.trainer
          );
      }

      return [
        ...new Set(
          names.filter(
            (
              name
            ): name is string =>
              Boolean(
                name?.trim()
              )
          )
        ),
      ];
    }, [
      race,
      last12MonthEntityType,
    ]);

  // Load Last 12 Months
  useEffect(() => {
    if (
      activeTab !==
        "last-12-months" ||
      !race ||
      !selectedDate ||
      last12MonthNames.length ===
        0
    ) {
      return;
    }

    async function loadStats() {
      try {
        setLoadingLast12Months(
          true
        );

        setLast12MonthError(
          ""
        );

        setLast12MonthStats(
          []
        );

        const response =
          await fetch(
            "/api/race-viewer/last-12-months",
            {
              method:
                "POST",

              headers: {
                "Content-Type":
                  "application/json",
              },

              body:
                JSON.stringify(
                  {
                    names:
                      last12MonthNames,

                    asOfDate:
                      selectedDate,

                    entityType:
                      last12MonthEntityType,
                  }
                ),
            }
          );

        if (!response.ok) {
          throw new Error();
        }

        const data =
          await response.json();

        if (!data.exchangeRate || !(data.exchangeRate.gbpToEur > 0)) {
          throw new Error("Exchange rate missing from response.");
        }
        setExchangeRate(data.exchangeRate as ExchangeRate);
        setLast12MonthStats(
          data.stats ?? []
        );
      } catch {
        setLast12MonthError(
          "Unable to load Last 12 Months data."
        );
      } finally {
        setLoadingLast12Months(
          false
        );
      }
    }

    loadStats();
  }, [
    activeTab,
    race,
    selectedDate,
    last12MonthEntityType,
    last12MonthNames,
  ]);

  const sortedLast12Stats = useMemo(() => {
    const percentage = (value: number, total: number) => total ? value / total : 0;
    const metric = (stat: Last12MonthStat, field: Last12SortField): number => {
      switch (field) {
        case "races": return stat.races;
        case "wins": return stat.wins;
        case "winPct": return percentage(stat.wins, stat.races);
        case "places": return stat.places;
        case "placePct": return percentage(stat.places, stat.races);
        case "combinedPct": return percentage(stat.wins + stat.places, stat.races);
        case "prizeMoneyTotal": return stat.prizeMoneyTotal;
        default: return 0;
      }
    };
    return [...last12MonthStats].sort((a, b) => {
      const comparison = last12SortField === "entity"
        ? a.entity.localeCompare(b.entity, "en-GB", { sensitivity: "base" })
        : metric(a, last12SortField) - metric(b, last12SortField);
      return (last12SortDirection === "asc" ? comparison : -comparison)
        || a.entity.localeCompare(b.entity, "en-GB", { sensitivity: "base" });
    });
  }, [last12MonthStats, last12SortField, last12SortDirection]);

  function handleLast12Sort(field: Last12SortField) {
    if (field === last12SortField) {
      setLast12SortDirection((current) => current === "asc" ? "desc" : "asc");
    } else {
      setLast12SortField(field);
      setLast12SortDirection(field === "entity" ? "asc" : "desc");
    }
  }

  function openHistory(stat: Last12MonthStat, category: HistoryCategory) {
    const qualifying = (stat.raceDetails ?? []).filter((item) =>
      category === "wins" ? item.won : category === "places" ? item.placed : item.won || item.placed
    );
    setHistorySelection({ entity: stat.entity, category, races: qualifying });
    setShowAllHistory(false);
  }

  function openHistoricalRace(item: Last12MonthRace) {
    setHistorySelection(null);
    setHistoryNavigationError("");
    if (!item.date || !item.location || !item.time) {
      setHistoryNavigationError("This historical race has no usable date, location or time.");
      return;
    }
    if (item.date === selectedDate) {
      const match = races.find((raceItem) =>
        raceItem.location.trim().toLowerCase() === item.location.trim().toLowerCase()
        && raceItem.time.trim() === item.time.trim()
      );
      if (!match) {
        setHistoryNavigationError("That historical race is not available in the Race Viewer race spine.");
        return;
      }
      setSelectedLocation(match.location);
      setSelectedTime(match.time);
      setExpandedMeetings(new Set([match.location]));
      setActiveTab("results");
    } else {
      pendingHistoricalRace.current = item;
      setSelectedLocation("");
      setSelectedTime("");
      setRace(null);
      setSelectedDate(item.date);
      setDates((current) => current.includes(item.date)
        ? current : [item.date, ...current].sort((a, b) => b.localeCompare(a)));
    }
  }

  function toggleMeeting(
    location: string
  ) {
    setExpandedMeetings(
      (current) => {
        const next =
          new Set(
            current
          );

        if (
          next.has(
            location
          )
        ) {
          next.delete(
            location
          );
        } else {
          next.add(
            location
          );
        }

        return next;
      }
    );
  }

  function selectRace(
    selectedRace: RaceOption
  ) {
    setSelectedLocation(
      selectedRace.location
    );

    setSelectedTime(
      selectedRace.time
    );

    setActiveTab(
      "pre-race"
    );

    setExpandedHorse(
      null
    );
  }

  function toggleHorse(
    runner: RaceRunner
  ) {
    const key =
      `${runner.horseNumber}-${runner.horseName}`;

    setExpandedHorse(
      (current) =>
        current === key
          ? null
          : key
    );
  }

  function handlePreRaceSort(
    field: PreRaceSortField
  ) {
    if (
      preRaceSortField ===
      field
    ) {
      setPreRaceSortDirection(
        (current) =>
          current === "asc"
            ? "desc"
            : "asc"
      );

      return;
    }

    setPreRaceSortField(
      field
    );

    setPreRaceSortDirection(
      "asc"
    );
  }

  function handleResultSort(field: ResultSortField) {
    if (resultSortField === field) {
      setResultSortDirection((current) => current === "asc" ? "desc" : "asc");
      return;
    }

    setResultSortField(field);
    setResultSortDirection("asc");
  }

  function toggleResultHorse(runner: RaceRunner) {
    const key = `${runner.postHorseNumber || runner.horseNumber}-${runner.postHorseName || runner.horseName}`;
    setExpandedResultHorse((current) => current === key ? null : key);
  }

  function formatPercentage(
    value: number,
    total: number
  ) {
    if (!total) {
      return "0.0%";
    }

    return `${(
      (value / total) *
      100
    ).toFixed(1)}%`;
  }

  function formatPrizeMoney(
    value: number
  ) {
    const converted =
      currency === "EUR"
        ? value * (exchangeRate?.gbpToEur ?? 1)
        : value;

    return new Intl.NumberFormat(
      "en-GB",
      {
        style:
          "currency",

        currency,

        maximumFractionDigits:
          0,
      }
    ).format(
      converted
    );
  }

  return (
    <div className="w-full">
      <div className="grid min-h-[calc(100vh-68px)] grid-cols-[270px_minmax(0,1fr)] lg:grid-cols-[305px_minmax(0,1fr)]">
        {/* SIDEBAR */}
        <aside className="border-r border-[var(--racing-gold-dark)] bg-[var(--racing-green)] text-white">
          {/* Date selector and calendar */}
          <div ref={calendarRef} className="relative border-b border-white/15 px-4 py-4">
            <div className="racing-small-caps mb-2 text-[10px] text-[var(--racing-gold-light)]">
              Race Date
            </div>
            <button
              type="button"
              onClick={() => {
                if (!calendarOpen && selectedDate) {
                  const [year, month] = selectedDate.split("-").map(Number);
                  setCalendarMonth(new Date(year, month - 1, 1));
                }
                setCalendarOpen((open) => !open);
              }}
              aria-expanded={calendarOpen}
              aria-label="Choose race date"
              className="flex w-full items-center justify-between gap-2 rounded border border-[var(--racing-gold-dark)] bg-[var(--racing-green-dark)] px-3 py-2 text-left text-[13px] text-white hover:border-[var(--racing-gold)]"
            >
              <span>{selectedDate ? formatDisplayDate(selectedDate) : "Select a date"}</span>
              <span aria-hidden="true" className="text-[var(--racing-gold-light)]">▦</span>
            </button>
            {calendarOpen && calendarMonth && (
              <div className="absolute left-4 right-4 top-[calc(100%-6px)] z-40 rounded border border-[var(--racing-gold)] bg-[var(--racing-green-dark)] p-3 shadow-2xl">
                <div className="mb-3 flex items-center justify-between">
                  <button type="button" aria-label="Previous month"
                    onClick={() => setCalendarMonth((month) => month ? new Date(month.getFullYear(), month.getMonth() - 1, 1) : month)}
                    className="rounded px-2 py-1 text-lg text-[var(--racing-gold-light)] hover:bg-white/10">‹</button>
                  <span className="racing-serif text-[17px] font-semibold text-white">
                    {new Intl.DateTimeFormat("en-GB", { month: "long", year: "numeric" }).format(calendarMonth)}
                  </span>
                  <button type="button" aria-label="Next month"
                    onClick={() => setCalendarMonth((month) => month ? new Date(month.getFullYear(), month.getMonth() + 1, 1) : month)}
                    className="rounded px-2 py-1 text-lg text-[var(--racing-gold-light)] hover:bg-white/10">›</button>
                </div>
                <div className="grid grid-cols-7 gap-1 text-center">
                  {["M", "T", "W", "T", "F", "S", "S"].map((day, index) => (
                    <div key={index} className="pb-1 text-[10px] text-[var(--racing-gold-light)]">{day}</div>
                  ))}
                  {Array.from({ length: (calendarMonth.getDay() + 6) % 7 }, (_, index) => (
                    <div key={`blank-${index}`} />
                  ))}
                  {Array.from({ length: new Date(calendarMonth.getFullYear(), calendarMonth.getMonth() + 1, 0).getDate() }, (_, index) => {
                    const day = index + 1;
                    const date = `${calendarMonth.getFullYear()}-${String(calendarMonth.getMonth() + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
                    const available = availableDates.has(date);
                    const selected = date === selectedDate;
                    const today = date === todayDate;
                    return (
                      <button key={date} type="button" disabled={!available}
                        onClick={() => chooseCalendarDate(date)}
                        title={available ? formatDisplayDate(date) : "No racing data available"}
                        aria-label={formatDisplayDate(date)}
                        aria-pressed={selected}
                        className={`flex h-8 items-center justify-center rounded text-[12px] transition-colors ${selected
                          ? "bg-[var(--racing-gold-light)] font-bold text-[var(--racing-green-dark)]"
                          : available
                            ? "text-white hover:bg-white/15"
                            : "cursor-not-allowed text-white/25"} ${today && !selected ? "ring-1 ring-inset ring-[var(--racing-gold)]" : ""}`}
                      >{day}</button>
                    );
                  })}
                </div>
                <div className="mt-3 grid grid-cols-2 gap-2 border-t border-white/15 pt-3">
                  {([ ["Today", 0], ["Yesterday", -1] ] as const).map(([label, offset]) => {
                    const date = offset === 0 ? todayDate : todayDate ? localDateString(-1) : null;
                    return (
                      <button key={label} type="button" disabled={!date || !availableDates.has(date)}
                        onClick={() => { if (date) chooseCalendarDate(date); }}
                        className="rounded border border-[var(--racing-gold-dark)] px-2 py-1.5 text-[12px] text-white enabled:hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-35"
                      >{label}</button>
                    );
                  })}
                </div>
              </div>
            )}
          </div>

          {/* Meetings */}
          <div className="px-4 py-4">
            <div className="racing-small-caps mb-3 text-[11px] text-[var(--racing-gold-light)]">
              Today&apos;s Racing
            </div>

            {loadingRaces ? (
              <div className="text-sm text-white/60">
                Loading meetings...
              </div>
            ) : (
              <div className="overflow-hidden rounded border border-[var(--racing-gold-dark)]">
                {locations.map(
                  (
                    location,
                    index
                  ) => {
                    const meetingRaces =
                      racesByLocation.get(
                        location
                      ) ?? [];

                    const expanded =
                      expandedMeetings.has(
                        location
                      );

                    return (
                      <div
                        key={
                          location
                        }
                        className={
                          index > 0
                            ? "border-t border-white/15"
                            : ""
                        }
                      >
                        {/* Meeting heading */}
                        <button
                          type="button"
                          onClick={() =>
                            toggleMeeting(
                              location
                            )
                          }
                          className="racing-serif flex h-9 w-full items-center justify-between px-3 text-left text-[15px] font-semibold hover:bg-white/5"
                        >
                          <span>
                            {
                              location
                            }{" "}
                            <span className="font-sans text-[10px] font-normal text-white/50">
                              (
                              {
                                meetingRaces.length
                              }
                              )
                            </span>
                          </span>

                          <span className="text-[var(--racing-gold)]">
                            {expanded
                              ? "⌄"
                              : "›"}
                          </span>
                        </button>

                        {/* Races */}
                        {expanded && (
                          <div className="border-t border-white/10">
                            {meetingRaces.map(
                              (
                                item
                              ) => {
                                const active =
                                  selectedLocation ===
                                    item.location &&
                                  selectedTime ===
                                    item.time;

                                return (
                                  <button
                                    key={
                                      item.raceKey
                                    }
                                    type="button"
                                    onClick={() =>
                                      selectRace(
                                        item
                                      )
                                    }
                                    title={
                                      item.raceName ??
                                      undefined
                                    }
                                    className={`grid min-h-10 w-full grid-cols-[45px_minmax(0,1fr)] items-center gap-2 border-b border-white/10 px-3 py-1.5 text-left last:border-b-0 ${
                                      active
                                        ? "bg-[var(--racing-gold-light)] text-[var(--racing-green-dark)]"
                                        : "text-white/90 hover:bg-white/5"
                                    }`}
                                  >
                                    <span className="racing-serif text-[14px] font-semibold">
                                      {
                                        item.time
                                      }
                                    </span>

                                    <span className="truncate text-[10px] leading-4 opacity-75">
                                      {item.raceName ||
                                        "Race"}
                                    </span>
                                  </button>
                                );
                              }
                            )}
                          </div>
                        )}
                      </div>
                    );
                  }
                )}
              </div>
            )}
          </div>

          {/* Pacey32 Racing horseshoe branding */}
          <div className="mx-4 mt-2 border-t border-[var(--racing-gold-dark)] py-4 text-center">
            <img
              src="/branding/pacey32-horseshoe.png"
              alt="Pacey32 Racing horseshoe"
              className="mx-auto h-24 w-24 object-contain"
            />

            <div className="mt-1 text-[9px] uppercase tracking-[0.3em] text-white/35">
              Pacey32 Racing
            </div>
          </div>
        </aside>

        {/* MAIN */}
        <main className="min-w-0 bg-[var(--racing-paper)]">
          {error && (
            <div className="m-6 border border-red-300 bg-red-50 p-4 text-sm text-red-800">
              {error}
            </div>
          )}

          {loadingRace && (
            <div className="p-7 text-sm text-[var(--racing-muted)]">
              Loading selected race...
            </div>
          )}

          {!loadingRace &&
            race && (
              <>
                {/* Breadcrumb */}
                <div className="border-b border-[var(--racing-border)] px-7 py-2.5 md:px-9">
                  <div className="racing-serif flex flex-wrap items-center gap-2.5 text-[13px] text-[var(--racing-muted)]">
                    <span>
                      Race Viewer
                    </span>

                    <span className="text-[var(--racing-gold)]">
                      ›
                    </span>

                    <span>
                      {
                        race.location
                      }
                    </span>

                    <span className="text-[var(--racing-gold)]">
                      ›
                    </span>

                    <span className="font-semibold text-[var(--racing-ink)]">
                      {
                        race.time
                      }
                    </span>
                  </div>
                </div>

                {/* Race header */}
                <section className="border-b border-[var(--racing-gold)] bg-[var(--racing-ivory)] px-7 py-4 md:px-9">
                  <div className="flex flex-col gap-2">
                    {/* Course and time */}
                    <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
                      <div className="racing-serif text-[28px] font-semibold leading-none text-[var(--racing-gold-dark)] md:text-[32px]">
                        {
                          race.time
                        }
                      </div>

                      <div className="hidden h-7 w-px bg-[var(--racing-gold)] md:block" />

                      <h1 className="racing-serif text-[28px] font-semibold leading-none text-[var(--racing-ink)] md:text-[32px]">
                        {
                          race.location
                        }
                      </h1>
                    </div>

                    {/* Race name */}
                    <h2 className="racing-serif max-w-6xl text-[20px] font-semibold leading-tight text-[var(--racing-ink)] md:text-[23px]">
                      {
                        race.name
                      }
                    </h2>

                    {/* Race date */}
                    <div className="racing-serif text-[13px] text-[var(--racing-muted)]">
                      {formatRaceDate(
                        race.date,
                        race.dayOfWeek
                      )}
                    </div>

                    {/* Race metadata */}
                    <div className="flex flex-wrap items-center gap-y-1 text-[12px] text-[var(--racing-ink)]">
                      {race.raceClass && (
                        <>
                          <span>
                            Class{" "}
                            {
                              race.raceClass
                            }
                          </span>

                          <span className="mx-2.5 text-[var(--racing-gold)]">
                            |
                          </span>
                        </>
                      )}

                      {race.distance && (
                        <>
                          <span>
                            {
                              race.distance
                            }
                          </span>

                          <span className="mx-2.5 text-[var(--racing-gold)]">
                            |
                          </span>
                        </>
                      )}

                      {(
                        activeTab ===
                          "results" &&
                        race.postGoing
                          ? race.postGoing
                          : race.going
                      ) && (
                        <span>
                          {activeTab ===
                            "results" &&
                          race.postGoing
                            ? race.postGoing
                            : race.going}
                        </span>
                      )}

                      {race.surface && (
                        <>
                          <span className="mx-2.5 text-[var(--racing-gold)]">
                            |
                          </span>

                          <span>
                            {
                              race.surface
                            }
                          </span>
                        </>
                      )}

                      {race.runners && (
                        <>
                          <span className="mx-2.5 text-[var(--racing-gold)]">
                            |
                          </span>

                          <span>
                            {
                              race.runners
                            }{" "}
                            runners
                          </span>
                        </>
                      )}

                      {activeTab ===
                        "results" &&
                        race.postWinningTime && (
                          <>
                            <span className="mx-2.5 text-[var(--racing-gold)]">
                              |
                            </span>

                            <span>
                              Winning time:{" "}
                              {
                                race.postWinningTime
                              }
                            </span>
                          </>
                        )}

                      {race.sourceUrl && (
                        <>
                          <span className="mx-2.5 text-[var(--racing-gold)]">
                            |
                          </span>

                          <a
                            href={
                              race.sourceUrl
                            }
                            target="_blank"
                            rel="noreferrer"
                            className="racing-serif text-[13px] font-medium text-[var(--racing-green)] underline decoration-[var(--racing-gold)] underline-offset-4 hover:text-[var(--racing-green-dark)]"
                          >
                            Sporting Life
                            racecard →
                          </a>
                        </>
                      )}
                    </div>
                  </div>
                </section>

                {/* Tabs */}
                <div className="border-b border-[var(--racing-border)] px-7 md:px-9">
                  <div className="flex overflow-x-auto">
                    {[
                      [
                        "pre-race",
                        "Pre Race",
                      ],
                      [
                        "results",
                        "Results",
                      ],
                      [
                        "last-12-months",
                        "Last 12 Months",
                      ],
                    ].map(
                      ([
                        value,
                        label,
                      ]) => (
                        <button
                          key={
                            value
                          }
                          type="button"
                          onClick={() =>
                            setActiveTab(
                              value as RaceTab
                            )
                          }
                          className={`racing-serif min-w-32 border-x border-t px-6 py-2 text-[14px] font-medium ${
                            activeTab ===
                            value
                              ? "border-[var(--racing-green)] bg-[var(--racing-green)] text-white"
                              : "border-[var(--racing-border)] bg-[var(--racing-ivory)] text-[var(--racing-ink)] hover:bg-[var(--racing-parchment)]"
                          }`}
                        >
                          {
                            label
                          }
                        </button>
                      )
                    )}
                  </div>
                </div>

                {/* PRE RACE */}
                {activeTab ===
                  "pre-race" && (
                  <div className="px-7 py-3 md:px-9">
                    <div className="overflow-x-auto">
                      <table className="w-full table-fixed border-collapse">
                        <thead>
                          <tr className="border-y border-[var(--racing-border)] text-left">
                            <SortableHeader
                              label="Odds Rank"
                              field="oddsRank"
                              activeField={
                                preRaceSortField
                              }
                              direction={
                                preRaceSortDirection
                              }
                              onSort={
                                handlePreRaceSort
                              }
                              className="w-[7%]"
                            />

                            <SortableHeader
                              label="No."
                              field="horseNumber"
                              activeField={
                                preRaceSortField
                              }
                              direction={
                                preRaceSortDirection
                              }
                              onSort={
                                handlePreRaceSort
                              }
                              className="w-[4%]"
                            />

                            {showStallNumber && (
                              <SortableHeader
                                label="Draw"
                                field="stallNumber"
                                activeField={
                                  preRaceSortField
                                }
                                direction={
                                  preRaceSortDirection
                                }
                                onSort={
                                  handlePreRaceSort
                                }
                                className="w-[4%]"
                              />
                            )}

                            <th className="racing-small-caps w-[6%] px-2 py-2.5 text-[9px] font-semibold text-[var(--racing-muted)]">
                              Silk
                            </th>

                            <SortableHeader
                              label="Horse / Form"
                              field="horseName"
                              activeField={
                                preRaceSortField
                              }
                              direction={
                                preRaceSortDirection
                              }
                              onSort={
                                handlePreRaceSort
                              }
                              className="w-[18%]"
                            />

                            <SortableHeader
                              label="Age"
                              field="age"
                              activeField={
                                preRaceSortField
                              }
                              direction={
                                preRaceSortDirection
                              }
                              onSort={
                                handlePreRaceSort
                              }
                              className="w-[5%]"
                            />

                            <SortableHeader
                              label="Weight"
                              field="weight"
                              activeField={
                                preRaceSortField
                              }
                              direction={
                                preRaceSortDirection
                              }
                              onSort={
                                handlePreRaceSort
                              }
                              className="w-[7%]"
                            />

                            <SortableHeader
                              label="Jockey"
                              field="jockey"
                              activeField={
                                preRaceSortField
                              }
                              direction={
                                preRaceSortDirection
                              }
                              onSort={
                                handlePreRaceSort
                              }
                              className="w-[16%]"
                            />

                            <SortableHeader
                              label="Trainer"
                              field="trainer"
                              activeField={
                                preRaceSortField
                              }
                              direction={
                                preRaceSortDirection
                              }
                              onSort={
                                handlePreRaceSort
                              }
                              className="w-[16%]"
                            />

                            <SortableHeader
                              label="Odds"
                              field="odds"
                              activeField={
                                preRaceSortField
                              }
                              direction={
                                preRaceSortDirection
                              }
                              onSort={
                                handlePreRaceSort
                              }
                              className="w-[8%]"
                            />

                            <th
                              className="w-[3%]"
                              aria-label="Expand"
                            />
                          </tr>
                        </thead>

                        <tbody>
                          {sortedPreRaceEntries.map(
                            (
                              runner
                            ) => {
                              const runnerKey =
                                `${runner.horseNumber}-${runner.horseName}`;

                              const expanded =
                                expandedHorse ===
                                runnerKey;

                              const history =
                                cleanRaceHistory(
                                  runner.raceHistoryStats
                                );

                              return (
                                <Fragment
                                  key={
                                    runnerKey
                                  }
                                >
                                  {/* Main runner row */}
                                  <tr
                                    onClick={() =>
                                      toggleHorse(
                                        runner
                                      )
                                    }
                                    className={`cursor-pointer border-b border-[var(--racing-border)] transition-colors ${
                                      expanded
                                        ? "bg-[var(--racing-gold-light)]/30"
                                        : "hover:bg-[var(--racing-ivory)]"
                                    }`}
                                  >
                                    {/* Odds rank */}
                                    <td className="px-2 py-3">
                                      {runner.oddsRank ? (
                                        <span
                                          className={`racing-serif flex h-7 w-7 items-center justify-center rounded-full text-[13px] font-semibold ${
                                            runner.oddsRank ===
                                            1
                                              ? "bg-[var(--racing-gold)] text-white"
                                              : "bg-[var(--racing-parchment)] text-[var(--racing-ink)]"
                                          }`}
                                        >
                                          {
                                            runner.oddsRank
                                          }
                                        </span>
                                      ) : (
                                        "—"
                                      )}
                                    </td>

                                    {/* Number */}
                                    <td className="racing-serif px-2 py-3 text-[16px] font-semibold">
                                      {
                                        runner.horseNumber
                                      }
                                    </td>

                                    {/* Draw */}
                                    {showStallNumber && (
                                      <td className="racing-serif px-2 py-3 text-[14px]">
                                        {runner.stallNumber && !["N/A", "NA", "-", "—"].includes(runner.stallNumber.trim().toUpperCase()) ? runner.stallNumber : "—"}
                                      </td>
                                    )}

                                    {/* Silk */}
                                    <td className="px-2 py-3">
                                      {runner.silkUrl ? (
                                        <img
                                          src={
                                            runner.silkUrl
                                          }
                                          alt={`${runner.horseName} silk`}
                                          className="h-10 w-10 object-contain"
                                        />
                                      ) : (
                                        "—"
                                      )}
                                    </td>

                                    {/* Horse / Form */}
                                    <td className="min-w-[170px] px-2 py-3">
                                      <div className="racing-serif text-[16px] font-semibold">
                                        {
                                          runner.horseName
                                        }
                                      </div>

                                      <div className="mt-0.5 text-[10px] text-[var(--racing-muted)]">
                                        Form:{" "}
                                        {runner.form &&
                                        runner.form.trim().toUpperCase() !== "N/A"
                                          ? runner.form
                                          : "None"}
                                      </div>
                                    </td>

                                    {/* Age */}
                                    <td className="px-2 py-3 text-[12px]">
                                      {runner.age
                                        ? `${runner.age}yo`
                                        : "—"}
                                    </td>

                                    {/* Weight */}
                                    <td className="whitespace-nowrap px-2 py-3 text-[12px]">
                                      {runner.weight ||
                                        "—"}
                                    </td>

                                    {/* Jockey */}
                                    <td className="px-2 py-3 text-[12px] leading-4">
                                      {runner.jockey ||
                                        "—"}
                                    </td>

                                    {/* Trainer */}
                                    <td className="px-2 py-3 text-[12px] leading-4">
                                      {runner.trainer ||
                                        "—"}
                                    </td>

                                    {/* Odds */}
                                    <td className="racing-serif bg-[var(--racing-gold-light)]/40 px-2 py-3 text-[16px] font-semibold">
                                      {runner.odds ||
                                        "—"}
                                    </td>

                                    {/* Expand */}
                                    <td className="px-2 py-3 text-center">
                                      <span
                                        className={`inline-block text-[13px] text-[var(--racing-gold-dark)] transition-transform ${
                                          expanded
                                            ? "rotate-180"
                                            : ""
                                        }`}
                                      >
                                        ⌄
                                      </span>
                                    </td>
                                  </tr>

                                  {/* Expanded runner detail */}
                                  {expanded && (
                                    <tr className="border-b border-[var(--racing-gold)] bg-[var(--racing-ivory)]">
                                      <td
                                        colSpan={
                                          showStallNumber
                                            ? 11
                                            : 10
                                        }
                                        className="px-5 py-4"
                                      >
                                        <div className="grid gap-6 lg:grid-cols-[minmax(0,3fr)_minmax(220px,1fr)]">
                                          {/* Commentary */}
                                          <div>
                                            <div className="racing-small-caps mb-2 text-[9px] font-semibold text-[var(--racing-gold-dark)]">
                                              Commentary
                                            </div>

                                            <p className="max-w-5xl text-[12px] leading-5 text-[var(--racing-ink)]">
                                              {runner.commentary ||
                                                "No pre-race commentary available."}
                                            </p>
                                          </div>

                                          {/* Additional details */}
                                          <div className="border-t border-[var(--racing-border)] pt-4 lg:border-l lg:border-t-0 lg:pl-6 lg:pt-0">
                                            <div className="racing-small-caps mb-3 text-[9px] font-semibold text-[var(--racing-gold-dark)]">
                                              Additional Details
                                            </div>

                                            <div className="grid grid-cols-[90px_1fr] gap-x-3 gap-y-2 text-[11px]">
                                              <span className="text-[var(--racing-muted)]">
                                                Race history
                                              </span>

                                              <span className="font-medium">
                                                {history || "—"}
                                              </span>

                                              <span className="text-[var(--racing-muted)]">
                                                Last run
                                              </span>

                                              <span className="font-medium">
                                                {runner.lastRun
                                                  ? `${runner.lastRun} days`
                                                  : "—"}
                                              </span>

                                              <span className="text-[var(--racing-muted)]">
                                                Headgear
                                              </span>

                                              <span className="font-medium">
                                                {runner.headgear || "—"}
                                              </span>
                                            </div>
                                          </div>
                                        </div>
                                      </td>
                                    </tr>
                                  )}
                                </Fragment>
                              );
                            }
                          )}
                        </tbody>
                      </table>
                    </div>

                    <div className="mt-2 text-right text-[9px] text-[var(--racing-muted)]">
                      Select a runner to view commentary and additional form details.
                    </div>
                  </div>
                )}

                {/* RESULTS */}
                {activeTab === "results" && (
                  <div className="px-7 py-4 md:px-9">
                    {!hasResults ? (
                      <div className="border-y border-[var(--racing-border)] py-10 text-center">
                        <h3 className="racing-serif text-xl font-semibold">Results not available yet</h3>
                        <p className="mt-2 text-sm text-[var(--racing-muted)]">This race does not currently have result data.</p>
                      </div>
                    ) : (
                      <div>
                        <div className="overflow-x-auto">
                          <table className="w-full table-fixed border-collapse">
                            <thead>
                              <tr className="border-y border-[var(--racing-border)] text-left">
                                <ResultSortableHeader label="Pos" field="position" activeField={resultSortField} direction={resultSortDirection} onSort={handleResultSort} className="w-[7%]" />
                                <ResultSortableHeader label="No." field="horseNumber" activeField={resultSortField} direction={resultSortDirection} onSort={handleResultSort} className="w-[6%]" />
                                {showResultStallNumber && (
                                  <ResultSortableHeader label="Draw" field="stallNumber" activeField={resultSortField} direction={resultSortDirection} onSort={handleResultSort} className="w-[6%]" />
                                )}
                                <th className="racing-small-caps w-[7%] px-2 py-2.5 text-[9px] font-semibold text-[var(--racing-muted)]">Silk</th>
                                <ResultSortableHeader label="Horse" field="horseName" activeField={resultSortField} direction={resultSortDirection} onSort={handleResultSort} className="w-[19%]" />
                                <ResultSortableHeader
                                  label="SP"
                                  field="sp"
                                  activeField={resultSortField}
                                  direction={resultSortDirection}
                                  onSort={handleResultSort}
                                  className="w-[8%] bg-[var(--racing-gold-light)]/35"
                                />
                                <ResultSortableHeader label="Prize Money" field="prizeMoney" activeField={resultSortField} direction={resultSortDirection} onSort={handleResultSort} className="w-[13%]" />
                                <ResultSortableHeader label="Jockey" field="jockey" activeField={resultSortField} direction={resultSortDirection} onSort={handleResultSort} className="w-[15%]" />
                                <ResultSortableHeader label="Trainer" field="trainer" activeField={resultSortField} direction={resultSortDirection} onSort={handleResultSort} className="w-[15%]" />
                                <th className="w-[4%]" aria-label="Expand" />
                              </tr>
                            </thead>

                            <tbody>
                              {resultEntries.map((runner) => {
                                const horseNumber = runner.postHorseNumber || runner.horseNumber;
                                const stallNumber = runner.postStallNumber || runner.stallNumber;
                                const horseName = runner.postHorseName || runner.horseName;
                                const key = `${horseNumber}-${horseName}`;
                                const expanded = expandedResultHorse === key;

                                return (
                                  <Fragment key={key}>
                                    <tr
                                      onClick={() => toggleResultHorse(runner)}
                                      className={`cursor-pointer border-b border-[var(--racing-border)] align-middle transition-colors hover:bg-[var(--racing-ivory)] ${expanded ? "bg-[var(--racing-ivory)]" : ""}`}
                                    >
                                      <td className="px-2 py-3">
                                        {runner.position ? (
                                          <span
                                            className={`racing-serif inline-flex h-10 w-10 items-center justify-center rounded-full text-[13px] font-semibold ${
                                              positionSortValue(runner.position) === 1
                                                ? "bg-[var(--racing-gold)] text-white"
                                                : "bg-[var(--racing-parchment)] text-[var(--racing-ink)]"
                                            }`}
                                          >
                                            {runner.position}
                                          </span>
                                        ) : (
                                          <span className="racing-serif text-[15px] font-semibold">
                                            {runner.result || "—"}
                                          </span>
                                        )}
                                      </td>
                                      <td className="px-2 py-3 text-[12px] font-medium">{horseNumber || "—"}</td>
                                      {showResultStallNumber && <td className="px-2 py-3 text-[12px]">{stallNumber && !["N/A", "NA", "-", "—"].includes(stallNumber.trim().toUpperCase()) ? stallNumber : "—"}</td>}
                                      <td className="px-2 py-3">
                                        {runner.silkUrl ? <img src={runner.silkUrl} alt={`${horseName} silk`} className="h-9 w-9 object-contain" /> : "—"}
                                      </td>
                                      <td className="racing-serif px-2 py-3 text-[16px] font-semibold">{horseName}</td>
                                      <td className="racing-serif bg-[var(--racing-gold-light)]/35 px-2 py-3 text-[15px] font-semibold">
                                        {runner.sp || "—"}
                                      </td>
                                      <td className="px-2 py-3 text-[12px]">{runner.prizeMoney && runner.prizeMoney !== "0" ? runner.prizeMoney : "—"}</td>
                                      <td className="px-2 py-3 text-[12px] leading-4">{runner.postJockey || runner.jockey || "—"}</td>
                                      <td className="px-2 py-3 text-[12px] leading-4">{runner.postTrainer || runner.trainer || "—"}</td>
                                      <td className="px-2 py-3 text-right text-[11px] text-[var(--racing-gold-dark)]">{expanded ? "▲" : "▼"}</td>
                                    </tr>

                                    {expanded && (
                                      <tr className="border-b border-[var(--racing-gold)] bg-[var(--racing-ivory)]">
                                        <td colSpan={showResultStallNumber ? 10 : 9} className="px-5 py-4">
                                          <div className="racing-small-caps mb-2 text-[9px] font-semibold text-[var(--racing-gold-dark)]">Ride Description</div>
                                          <p className="max-w-5xl text-[12px] leading-5 text-[var(--racing-ink)]">{runner.rideDescription || "No ride description available."}</p>
                                        </td>
                                      </tr>
                                    )}
                                  </Fragment>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                        <div className="mt-2 text-right text-[9px] text-[var(--racing-muted)]">Select a runner to view the ride description.</div>
                      </div>
                    )}
                  </div>
                )}

                {/* LAST 12 MONTHS */}
                {activeTab ===
                  "last-12-months" && (
                  <div className="px-7 py-5 md:px-9">
                    <div className="mb-5 flex flex-col justify-between gap-4 border-b border-[var(--racing-border)] pb-4 md:flex-row md:items-end">
                      <div>
                        <h3 className="racing-serif text-[22px] font-semibold">
                          Last 12 Months
                        </h3>

                        <p className="mt-1 text-[12px] text-[var(--racing-muted)]">
                          Performance in
                          the twelve
                          months up to{" "}
                          {formatDisplayDate(
                            selectedDate
                          )}
                          .
                        </p>
                      </div>

                      <div className="flex flex-wrap gap-3">
                        {/* Entity selector */}
                        <div className="flex overflow-hidden border border-[var(--racing-border)]">
                          {[
                            [
                              "HORSE",
                              "Horses",
                            ],
                            [
                              "JOCKEY",
                              "Jockeys",
                            ],
                            [
                              "TRAINER",
                              "Trainers",
                            ],
                          ].map(
                            ([
                              value,
                              label,
                            ]) => (
                              <button
                                key={
                                  value
                                }
                                type="button"
                                onClick={() => {
                                  setHistorySelection(null);
                                  setLast12MonthEntityType(value as Last12MonthEntityType);
                                }}
                                className={`px-3.5 py-1.5 text-[12px] ${
                                  last12MonthEntityType ===
                                  value
                                    ? "bg-[var(--racing-green)] text-white"
                                    : "bg-[var(--racing-ivory)] text-[var(--racing-ink)] hover:bg-[var(--racing-parchment)]"
                                }`}
                              >
                                {
                                  label
                                }
                              </button>
                            )
                          )}
                        </div>

                        {/* Currency selector */}
                        <div className="flex overflow-hidden border border-[var(--racing-border)]">
                          {[
                            [
                              "GBP",
                              "£ GBP",
                            ],
                            [
                              "EUR",
                              "€ EUR",
                            ],
                          ].map(
                            ([
                              value,
                              label,
                            ]) => (
                              <button
                                key={
                                  value
                                }
                                type="button"
                                onClick={() =>
                                  setCurrency(
                                    value as Currency
                                  )
                                }
                                className={`px-3.5 py-1.5 text-[12px] ${
                                  currency ===
                                  value
                                    ? "bg-[var(--racing-green)] text-white"
                                    : "bg-[var(--racing-ivory)] text-[var(--racing-ink)] hover:bg-[var(--racing-parchment)]"
                                }`}
                              >
                                {
                                  label
                                }
                              </button>
                            )
                          )}
                        </div>
                      </div>
                    </div>

                    {exchangeRate && (
                      <p className="mb-3 text-right text-[11px] text-[var(--racing-muted)]">
                        {currency === "GBP"
                          ? `€1 = £${exchangeRate.eurToGbp.toFixed(4)}`
                          : `£1 = €${exchangeRate.gbpToEur.toFixed(4)}`}
                        {` · ECB · ${formatDisplayDate(exchangeRate.date)}`}
                      </p>
                    )}

                    {last12MonthError && (
                      <div className="mb-4 border border-red-300 bg-red-50 p-4 text-sm text-red-800">
                        {
                          last12MonthError
                        }
                      </div>
                    )}

                    {loadingLast12Months ? (
                      <p className="text-sm text-[var(--racing-muted)]">
                        Loading...
                      </p>
                    ) : last12MonthStats.length ===
                      0 ? (
                      <p className="py-8 text-sm text-[var(--racing-muted)]">
                        No Last 12 Months
                        data found.
                      </p>
                    ) : (
                      <div className="overflow-x-auto">
                        <table className="w-full min-w-[800px] table-fixed border-collapse">
                          <thead>
                            <tr className="border-y border-[var(--racing-border)]">
                              {([
                                [last12MonthEntityType === "HORSE" ? "Horse" : last12MonthEntityType === "JOCKEY" ? "Jockey" : "Trainer", "entity", "w-[25%]"],
                                ["Races", "races", "w-[10%]"],
                                ["Wins", "wins", "w-[14%]"],
                                ["Places", "places", "w-[14%]"],
                                ["Wins + Places", "combinedPct", "w-[17%]"],
                                ["Total Prize Money", "prizeMoneyTotal", "w-[20%]"],
                              ] as [string, Last12SortField, string][]).map(([label, field, width]) => (
                                <Last12SortableHeader key={field} label={label} field={field}
                                  className={width} activeField={last12SortField}
                                  direction={last12SortDirection} onSort={handleLast12Sort} />
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {sortedLast12Stats.map((stat) => (
                              <tr key={stat.entity} className="border-b border-[var(--racing-border)] hover:bg-[var(--racing-ivory)]">
                                <td className="racing-serif px-2 py-3 text-[16px] font-semibold">{stat.entity}</td>
                                <td className="px-2 py-3 text-[12px]">{stat.races}</td>
                                <td className="px-2 py-3 text-[12px]">
                                  {stat.wins ? (
                                    <button type="button" onClick={() => openHistory(stat, "wins")}
                                      className="text-left text-[var(--racing-green)] hover:text-[var(--racing-gold-dark)]"
                                      title={`Show ${stat.entity}'s wins`}>
                                      <span className="font-semibold underline decoration-[var(--racing-gold)] underline-offset-4">{stat.wins}</span>{" "}
                                      <span className="text-[var(--racing-muted)]">({formatPercentage(stat.wins, stat.races)})</span>
                                    </button>
                                  ) : <span>0 <span className="text-[var(--racing-muted)]">(0.0%)</span></span>}
                                </td>
                                <td className="px-2 py-3 text-[12px]">
                                  {stat.places ? (
                                    <button type="button" onClick={() => openHistory(stat, "places")}
                                      className="text-left text-[var(--racing-green)] hover:text-[var(--racing-gold-dark)]"
                                      title={`Show ${stat.entity}'s places`}>
                                      <span className="font-semibold underline decoration-[var(--racing-gold)] underline-offset-4">{stat.places}</span>{" "}
                                      <span className="text-[var(--racing-muted)]">({formatPercentage(stat.places, stat.races)})</span>
                                    </button>
                                  ) : <span>0 <span className="text-[var(--racing-muted)]">(0.0%)</span></span>}
                                </td>
                                <td className="px-2 py-3 text-[12px]">
                                  {stat.wins + stat.places ? (
                                    <button type="button" onClick={() => openHistory(stat, "combined")}
                                      className="text-left text-[var(--racing-green)] hover:text-[var(--racing-gold-dark)]"
                                      title={`Show ${stat.entity}'s wins and places`}>
                                      <span className="font-semibold underline decoration-[var(--racing-gold)] underline-offset-4">{stat.wins + stat.places}</span>{" "}
                                      <span className="text-[var(--racing-muted)]">({formatPercentage(stat.wins + stat.places, stat.races)})</span>
                                    </button>
                                  ) : <span>0 <span className="text-[var(--racing-muted)]">(0.0%)</span></span>}
                                </td>
                                <td className="racing-serif bg-[var(--racing-gold-light)]/40 px-2 py-3 text-[16px] font-semibold">{formatPrizeMoney(stat.prizeMoneyTotal)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}

                    <div className="mt-2 text-right text-[10px] text-[var(--racing-muted)]">
                      Select a Wins, Places or Wins + Places count to view the individual races.
                    </div>

                    <p className="mt-4 text-[10px] text-[var(--racing-muted)]">
                      Prize money in GBP and EUR is converted using the latest available ECB reference rate,
                      regardless of the race date. Values are not inflation-adjusted.
                    </p>
                  </div>
                )}
              </>
            )}

          {!loadingRace &&
            !race &&
            !error && (
              <div className="p-9">
                <h1 className="racing-serif text-3xl font-semibold">
                  Race Viewer
                </h1>

                <p className="mt-2 text-sm text-[var(--racing-muted)]">
                  Select a race from the racecard.
                </p>
              </div>
            )}
        </main>
      </div>

      {historyNavigationError && (
        <div role="alert" className="fixed bottom-5 right-5 z-40 max-w-md border border-red-300 bg-red-50 p-4 text-sm text-red-800 shadow-lg">
          {historyNavigationError}
          <button type="button" className="ml-4 underline" onClick={() => setHistoryNavigationError("")}>Dismiss</button>
        </div>
      )}

      {historySelection && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
          onMouseDown={(event) => { if (event.target === event.currentTarget) setHistorySelection(null); }}>
          <div role="dialog" aria-modal="true" aria-label="Historical race results"
            className="flex max-h-[85vh] w-full max-w-[700px] flex-col border border-[var(--racing-gold)] bg-[var(--racing-paper)] shadow-2xl">
            <div className="flex items-start justify-between border-b border-[var(--racing-border)] bg-[var(--racing-ivory)] px-5 py-4">
              <div>
                <h3 className="racing-serif text-[23px] font-semibold">{historySelection.entity}</h3>
                <p className="mt-1 text-[12px] text-[var(--racing-muted)]">
                  {historySelection.races.length} {historySelection.category === "wins" ? "wins" : historySelection.category === "places" ? "places" : "wins + places"} in the last 12 months
                </p>
              </div>
              <button type="button" onClick={() => setHistorySelection(null)}
                className="px-2 text-2xl text-[var(--racing-muted)] hover:text-[var(--racing-green)]" aria-label="Close">×</button>
            </div>
            <div className="overflow-y-auto px-5 py-3">
              {historySelection.races.length === 0 ? (
                <p className="py-5 text-sm text-[var(--racing-muted)]">No qualifying race records available.</p>
              ) : (
                <table className="w-full border-collapse text-left text-[12px]">
                  <thead><tr className="racing-small-caps border-b border-[var(--racing-border)] text-[10px] text-[var(--racing-muted)]">
                    <th className="py-2">Date</th><th className="py-2">Meeting</th><th className="py-2">Time</th><th className="py-2">Pos</th><th className="py-2">Race</th>
                  </tr></thead>
                  <tbody>
                    {(showAllHistory ? historySelection.races : historySelection.races.slice(0, 5)).map((item, index) => (
                      <tr key={`${item.raceId ?? item.date}-${item.time}-${index}`} className="border-b border-[var(--racing-border)]">
                        <td className="whitespace-nowrap py-3 pr-3">{item.date.split("-").reverse().join("/")}</td>
                        <td className="py-3 pr-3">{item.location}</td>
                        <td className="py-3 pr-3">{item.time}</td>
                        <td className="py-3 pr-3">{item.position ?? "—"}</td>
                        <td className="py-3">
                          <button type="button" onClick={() => openHistoricalRace(item)}
                            className="text-left font-medium text-[var(--racing-green)] underline decoration-[var(--racing-gold)] underline-offset-4 hover:text-[var(--racing-gold-dark)]"
                            title="Open this race in Results">
                            {item.raceName || "View Results"} →
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
            {historySelection.races.length > 5 && (
              <div className="border-t border-[var(--racing-border)] px-5 py-3">
                <button type="button" onClick={() => setShowAllHistory((value) => !value)}
                  className="text-[12px] font-semibold text-[var(--racing-green)] underline decoration-[var(--racing-gold)] underline-offset-4">
                  {showAllHistory ? "Show latest 5" : `View all ${historySelection.races.length} ${historySelection.category === "combined" ? "wins + places" : historySelection.category}`} →
                </button>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}