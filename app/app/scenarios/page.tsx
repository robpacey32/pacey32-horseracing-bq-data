
"use client";

import { useEffect, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";

import { auth } from "@/lib/firebase";
import { saveCloudScenario } from "@/lib/scenarios/cloudStorage";

import ScenarioSelections from "@/components/scenarios/ScenarioSelections";
import SavedScenarios, {
  formatScenarioFilters,
} from "@/components/scenarios/SavedScenarios";
import ModelledScenarios, {
  formatModelConditions,
  modelStrategyId,
  type ModelStrategy,
} from "@/components/scenarios/ModelledScenarios";
import ScenarioPerformance from "@/components/scenarios/ScenarioPerformance";
import type { PerformanceSelection } from "@/components/scenarios/ScenarioPerformance";

import type {
  ScenarioDefinition,
  ScenarioFilters,
  ScenarioResult,
} from "@/lib/scenarios/types";

type BooleanChoice = "any" | "yes" | "no";

function optionalNumber(value: string): number | undefined {
  if (value.trim() === "") return undefined;
  return Number(value);
}

function optionalBoolean(
  value: BooleanChoice
): boolean | undefined {
  if (value === "any") return undefined;
  return value === "yes";
}

function toBooleanChoice(value?: boolean): BooleanChoice {
  if (value === undefined) return "any";
  return value ? "yes" : "no";
}

function Help({ text }: { text: string }) {
  return (
    <span className="group relative ml-1 inline-flex align-middle">
      <span
        tabIndex={0}
        role="img"
        aria-label={`Information: ${text}`}
        className="cursor-help rounded-full text-xs font-normal text-gray-500 outline-offset-2 focus:outline focus:outline-2 focus:outline-[#123d2d]"
      >
        ⓘ
      </span>
      <span
        role="tooltip"
        className="pointer-events-none absolute bottom-full left-1/2 z-50 mb-2 hidden w-60 -translate-x-1/2 rounded-md bg-[#123d2d] px-3 py-2 text-left text-xs font-normal leading-relaxed text-white shadow-lg group-hover:block group-focus-within:block"
      >
        {text}
      </span>
    </span>
  );
}

function Field({
  label,
  help,
  children,
}: {
  label: string;
  help?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="block min-w-0 text-xs">
      <span className="mb-1 block font-semibold">
        {label}
        {help && <Help text={help} />}
      </span>
      {children}
    </label>
  );
}

function ChoiceSwitch({
  value,
  onChange,
}: {
  value: BooleanChoice;
  onChange: (value: BooleanChoice) => void;
}) {
  return (
    <div className="inline-flex w-fit items-center rounded border border-gray-300 bg-gray-50 p-px">
      {(["no", "any", "yes"] as const).map((option, index) => (
        <button
          key={option}
          type="button"
          onClick={() => onChange(option)}
          aria-pressed={value === option}
          style={{
            fontSize: "11px",
            lineHeight: "16px",
            padding: "2px 6px",
          }}
          className={`rounded-sm px-1.5 py-0.5 text-[11px] leading-4 font-medium whitespace-nowrap ${
            value === option
              ? "bg-[#123d2d] text-white"
              : "text-gray-600 hover:bg-white"
          }`}
        >
          {["Exclude", "Any", "Only"][index]}
        </button>
      ))}
    </div>
  );
}

function fractional(decimal: string) {
  const value = Number(decimal);

  if (
    !decimal.trim() ||
    !Number.isFinite(value) ||
    value < 1
  ) {
    return "";
  }

  const profit = value - 1;
  let bestNum = 0;
  let bestDen = 1;
  let bestDiff = Infinity;

  for (let den = 1; den <= 100; den++) {
    const num = Math.round(profit * den);
    const diff = Math.abs(profit - num / den);

    if (diff < bestDiff) {
      bestDiff = diff;
      bestNum = num;
      bestDen = den;
    }
  }

  const gcd = (a: number, b: number): number =>
    b ? gcd(b, a % b) : a;

  const factor = gcd(bestNum, bestDen) || 1;

  return `${bestNum / factor}/${bestDen / factor}`;
}

function RangeField({
  title,
  min,
  max,
  setMin,
  setMax,
  step = "1",
  hint,
  showFractional = false,
}: {
  title: string;
  min: string;
  max: string;
  setMin: (value: string) => void;
  setMax: (value: string) => void;
  step?: string;
  hint?: string;
  showFractional?: boolean;
}) {
  return (
    <div className="min-w-0 text-xs">
      <div className="mb-1 font-semibold">
        {title}
        {hint && <Help text={hint} />}
      </div>

      <div className="grid grid-cols-2 gap-1.5">
        {(
          [
            [min, setMin, "Min"],
            [max, setMax, "Max"],
          ] as Array<
            [string, (value: string) => void, string]
          >
        ).map(([value, setter, placeholder]) => (
          <div
            key={placeholder}
            className="flex min-w-0 items-center gap-1"
          >
            <input
              aria-label={`${title} ${placeholder.toLowerCase()}`}
              placeholder={placeholder}
              type="number"
              step={step}
              value={value}
              onChange={(e) => setter(e.target.value)}
              className="w-full min-w-0 rounded-md border border-gray-300 px-2 py-1.5 text-sm"
            />

            {showFractional && (
              <span className="shrink-0 whitespace-nowrap text-xs text-gray-500">
                ({fractional(value) || "—"})
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function MultiSelectDropdown({
  title,
  options,
  selected,
  onChange,
  loading = false,
}: {
  title: string;
  options: string[];
  selected: string[];
  onChange: (values: string[]) => void;
  loading?: boolean;
}) {
  const [search, setSearch] = useState("");

  const values = Array.from(
    new Set([...options, ...selected])
  );

  const visible = values.filter((value) =>
    value
      .toLocaleLowerCase()
      .includes(search.trim().toLocaleLowerCase())
  );

  const toggle = (value: string) =>
    onChange(
      selected.includes(value)
        ? selected.filter((item) => item !== value)
        : [...selected, value]
    );

  return (
    <div className="min-w-0 text-xs">
      <p className="mb-1 font-semibold">
        {title}
        <Help text="Leave unselected to include all values. Select one or more to filter." />
      </p>

      <details className="group relative rounded-md border border-gray-300 bg-white">
        <summary className="flex cursor-pointer list-none items-center justify-between gap-2 px-2 py-1.5 text-sm marker:hidden [&::-webkit-details-marker]:hidden">
          <span className="truncate">
            {selected.length === 0
              ? "All"
              : `${selected.length} selected`}
          </span>
          <span className="text-gray-500">▾</span>
        </summary>

        <div className="absolute left-0 right-0 top-full z-30 mt-1 min-w-[220px] rounded-md border border-gray-300 bg-white p-2 shadow-xl">
          <input
            type="search"
            value={search}
            onChange={(event) =>
              setSearch(event.target.value)
            }
            placeholder={`Search ${title.toLowerCase()}…`}
            aria-label={`Search ${title}`}
            className="mb-2 w-full rounded border border-gray-300 px-2 py-1 text-sm"
          />

          <div className="mb-2 flex justify-between text-xs">
            <button
              type="button"
              onClick={() => onChange(values)}
              className="font-medium text-[#123d2d]"
            >
              Select all
            </button>
            <button
              type="button"
              onClick={() => onChange([])}
              className="text-gray-600"
            >
              Clear all
            </button>
          </div>

          <div className="max-h-48 space-y-1 overflow-y-auto">
            {visible.map((value) => (
              <label
                key={value}
                className="flex cursor-pointer items-center gap-2 rounded px-1 py-1 text-xs hover:bg-gray-50"
              >
                <input
                  type="checkbox"
                  checked={selected.includes(value)}
                  onChange={() => toggle(value)}
                  className="accent-[#123d2d]"
                />
                {value}
              </label>
            ))}

            {!visible.length && (
              <p className="text-xs text-gray-500">
                {loading
                  ? "Loading options…"
                  : "No matching values"}
              </p>
            )}
          </div>
        </div>
      </details>
    </div>
  );
}

function MultiCheckbox({
  title,
  options,
  selected,
  onChange,
}: {
  title: string;
  options: string[];
  selected: string[];
  onChange: (items: string[]) => void;
}) {
  return (
    <div className="min-w-0 text-xs">
      <p className="mb-1 font-semibold">
        {title}
        <Help text="Select race types to include. No selection includes all types." />
      </p>

      <div className="flex min-h-[34px] flex-wrap items-center gap-x-3 gap-y-1 rounded-md border border-gray-200 bg-gray-50 px-2 py-1">
        {options.map((option) => (
          <label
            key={option}
            className="flex cursor-pointer items-center gap-1 text-xs"
          >
            <input
              type="checkbox"
              checked={selected.includes(option)}
              onChange={(e) =>
                onChange(
                  e.target.checked
                    ? [...selected, option]
                    : selected.filter((v) => v !== option)
                )
              }
              className="accent-[#123d2d]"
            />
            {option}
          </label>
        ))}
      </div>
    </div>
  );
}

const COURSES = [
  "Aintree", "Ascot", "Ayr", "Bangor", "Bath",
  "Beverley", "Brighton", "Carlisle", "Cartmel",
  "Catterick", "Chelmsford", "Cheltenham",
  "Chepstow", "Chester", "Clonmel", "Curragh",
  "Doncaster", "Dundalk", "Epsom", "Exeter",
  "Fairyhouse", "Ffos Las", "Fontwell", "Galway",
  "Goodwood", "Hamilton", "Haydock", "Hexham",
  "Huntingdon", "Kempton", "Leicester",
  "Leopardstown", "Limerick", "Lingfield",
  "Listowel", "Ludlow", "Market Rasen",
  "Musselburgh", "Naas", "Navan", "Newbury",
  "Newcastle", "Newmarket", "Nottingham",
  "Perth", "Pontefract", "Punchestown",
  "Redcar", "Ripon", "Roscommon", "Salisbury",
  "Sandown", "Sedgefield", "Southwell",
  "Stratford", "Taunton", "Thirsk", "Thurles",
  "Tipperary", "Uttoxeter", "Warwick",
  "Wetherby", "Wincanton", "Windsor",
  "Wolverhampton", "Worcester", "Yarmouth",
  "York",
];

const GOINGS = [
  "Heavy",
  "Soft to Heavy",
  "Soft",
  "Yielding to Soft",
  "Good to Soft",
  "Yielding",
  "Good to Yielding",
  "Good",
  "Good to Firm",
  "Firm",
  "Standard / Slow",
  "Standard",
];

const CLASSES = [
  "Class 1",
  "Class 2",
  "Class 3",
  "Class 4",
  "Class 5",
  "Class 6",
  "Class 7",
];

const SURFACES = [
  "Turf",
  "All Weather",
  "All-Weather",
  "AW",
  "Tapeta",
  "Polytrack",
  "Fibresand",
  "Dirt",
  "Grass",
];

function orderedGoings(values: string[]): string[] {
  const order = GOINGS.map((value) => value.toLowerCase());

  return [...values].sort((a, b) => {
    const ai = order.indexOf(a.toLowerCase());
    const bi = order.indexOf(b.toLowerCase());

    if (ai < 0 && bi < 0) return a.localeCompare(b);
    if (ai < 0) return 1;
    if (bi < 0) return -1;

    return ai - bi;
  });
}

export default function ScenariosPage() {
  // Dates

  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");

  useEffect(() => {
    const today = new Date();
    const start = new Date(today);

    start.setMonth(start.getMonth() - 3);

    setStartDate(start.toISOString().slice(0, 10));
    setEndDate(today.toISOString().slice(0, 10));
  }, []);

  // Live filter options

  const [filterOptions, setFilterOptions] = useState<{
    courses: string[];
    raceClasses: string[];
    goings: string[];
    surfaces: string[];
  } | null>(null);

  const [optionsError, setOptionsError] = useState(false);

  useEffect(() => {
    const controller = new AbortController();

    fetch("/api/scenarios/options", {
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          throw new Error("Could not load filter options");
        }
        return response.json();
      })
      .then((data) => setFilterOptions(data))
      .catch((error) => {
        if (error.name !== "AbortError") {
          setOptionsError(true);
        }
      });

    return () => controller.abort();
  }, []);

  // Scenario settings

  const [scenarioName, setScenarioName] =
    useState("Scenario 1");

  const [stake, setStake] = useState(10);

  const [betType, setBetType] = useState<
    "win" | "each_way"
  >("win");

  const [stakeAllocation, setStakeAllocation] = useState<
    "per_horse" | "per_race"
  >("per_horse");

  const [stakeMode, setStakeMode] = useState<
    "total" | "per_leg"
  >("total");

  // Betting filters

  const [minOdds, setMinOdds] = useState("2");
  const [maxOdds, setMaxOdds] = useState("10");

  const [minPreRaceRank, setMinPreRaceRank] = useState("");
  const [maxPreRaceRank, setMaxPreRaceRank] = useState("3");

  const [favouriteStatus, setFavouriteStatus] = useState<
    "any" | "favourite" | "not_favourite"
  >("any");

  // Race filters

  const [raceTypes, setRaceTypes] = useState<string[]>([
    "Flat",
  ]);

  const [courses, setCourses] = useState<string[]>([]);
  const [raceClasses, setRaceClasses] = useState<string[]>([]);
  const [goings, setGoings] = useState<string[]>([]);
  const [surfaces, setSurfaces] = useState<string[]>([]);

  const [minDistance, setMinDistance] = useState("");
  const [maxDistance, setMaxDistance] = useState("");

  const [handicap, setHandicap] =
    useState<BooleanChoice>("any");

  // Horse history

  const [courseWinner, setCourseWinner] =
    useState<BooleanChoice>("any");

  const [distanceWinner, setDistanceWinner] =
    useState<BooleanChoice>("any");

  const [courseDistanceWinner, setCourseDistanceWinner] =
    useState<BooleanChoice>("any");

  const [beatenFavourite, setBeatenFavourite] =
    useState<BooleanChoice>("any");

  // Recent form

  const [wonInLast3, setWonInLast3] =
    useState<BooleanChoice>("any");

  const [win2InLast6, setWin2InLast6] =
    useState<BooleanChoice>("any");

  // Each-way

  const [eachWayEligible, setEachWayEligible] =
    useState<BooleanChoice>("any");

  // Comparison state

  type SelectedItem = {
    selection: PerformanceSelection;
    definition: ScenarioDefinition | null;
  };

  const [selectedItems, setSelectedItems] =
    useState<SelectedItem[]>([]);

  const [comparisonStart, setComparisonStart] =
    useState("");

  const [comparisonEnd, setComparisonEnd] =
    useState("");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const requestRef = useRef(0);

  // Panel visibility

  const [builderOpen, setBuilderOpen] = useState(true);
  const [modelOpen, setModelOpen] = useState(true);
  const [savedOpen, setSavedOpen] = useState(true);

  // Saved scenario editing and account state

  const [editingScenarioId, setEditingScenarioId] =
    useState<string | null>(null);

  const [currentUser, setCurrentUser] =
    useState<User | null>(null);

  const [savingScenario, setSavingScenario] =
    useState(false);

  const [saveMessage, setSaveMessage] =
    useState<string | null>(null);

  const [saveError, setSaveError] =
    useState<string | null>(null);

  const [savedRevision, setSavedRevision] = useState(0);

  useEffect(() => {
    return onAuthStateChanged(auth, (user) => {
      setCurrentUser(user);
    });
  }, []);

  function toggleSelection(
    selection: PerformanceSelection,
    definition: ScenarioDefinition | null = null
  ) {
    requestRef.current++;
    setLoading(false);
    setError(null);

    setSelectedItems((previous) => {
      if (
        previous.some(
          (item) => item.selection.id === selection.id
        )
      ) {
        return previous.filter(
          (item) => item.selection.id !== selection.id
        );
      }

      return [...previous, { selection, definition }];
    });

    setComparisonStart(
      (previous) => previous || selection.startDate
    );

    setComparisonEnd(
      (previous) => previous || selection.endDate
    );
  }

  function removeScenario(id: string) {
    requestRef.current++;
    setLoading(false);
    setError(null);

    setSelectedItems((previous) =>
      previous.filter(
        (item) => item.selection.id !== id
      )
    );
  }

  function resetBuilderFilters() {
    setMinOdds("2");
    setMaxOdds("10");
    setMinPreRaceRank("");
    setMaxPreRaceRank("3");
    setFavouriteStatus("any");
    setRaceTypes(["Flat"]);
    setCourses([]);
    setRaceClasses([]);
    setGoings([]);
    setSurfaces([]);
    setMinDistance("");
    setMaxDistance("");
    setHandicap("any");
    setCourseWinner("any");
    setDistanceWinner("any");
    setCourseDistanceWinner("any");
    setBeatenFavourite("any");
    setWonInLast3("any");
    setWin2InLast6("any");
    setEachWayEligible("any");
  }

  function builderFilters(): ScenarioFilters {
    const filters: ScenarioFilters = {};

    const numbers = {
      minOdds: optionalNumber(minOdds),
      maxOdds: optionalNumber(maxOdds),
      minPreRaceRank: optionalNumber(minPreRaceRank),
      maxPreRaceRank: optionalNumber(maxPreRaceRank),
      minDistance: optionalNumber(minDistance),
      maxDistance: optionalNumber(maxDistance),
    };

    for (const [key, value] of Object.entries(numbers)) {
      if (value !== undefined) {
        Object.assign(filters, { [key]: value });
      }
    }

    if (raceTypes.length) filters.raceTypes = raceTypes;
    if (courses.length) filters.courses = courses;

    if (raceClasses.length) {
      filters.raceClasses = raceClasses;
    }

    if (goings.length) filters.goings = goings;
    if (surfaces.length) filters.surfaces = surfaces;

    if (favouriteStatus !== "any") {
      filters.favouriteStatus = favouriteStatus;
    }

    const booleans = {
      handicap: optionalBoolean(handicap),
      courseWinner: optionalBoolean(courseWinner),
      distanceWinner: optionalBoolean(distanceWinner),
      courseDistanceWinner:
        optionalBoolean(courseDistanceWinner),
      beatenFavourite:
        optionalBoolean(beatenFavourite),
      wonInLast3: optionalBoolean(wonInLast3),
      win2InLast6: optionalBoolean(win2InLast6),
      eachWayEligible:
        optionalBoolean(eachWayEligible),
    };

    for (const [key, value] of Object.entries(booleans)) {
      if (value !== undefined) {
        Object.assign(filters, { [key]: value });
      }
    }

    return filters;
  }

  function buildDefinition(): ScenarioDefinition {
    return {
      id: crypto.randomUUID(),
      name: scenarioName.trim() || "Custom Scenario",
      startDate,
      endDate,
      stake,
      betType,
      stakeAllocation,
      stakeMode,
      filters: builderFilters(),
    };
  }

  // Save the current builder settings.
  // Existing scenarios retain their original Firestore ID.

  async function handleSaveScenario() {
    if (savingScenario) return;

    setSaveMessage(null);
    setSaveError(null);

    const user = auth.currentUser;

    if (!user) {
      setSaveError("Sign in to save a scenario.");
      return;
    }

    if (
      !scenarioName.trim() ||
      scenarioName.trim().length > 80
    ) {
      setSaveError(
        "Enter a scenario name (1–80 characters)."
      );
      return;
    }

    if (
      !Number.isFinite(stake) ||
      stake <= 0 ||
      !startDate ||
      !endDate ||
      startDate > endDate
    ) {
      setSaveError(
        "Enter a valid stake and date range."
      );
      return;
    }

    const definition: ScenarioDefinition = {
      ...buildDefinition(),
      id: editingScenarioId ?? crypto.randomUUID(),
      name: scenarioName.trim(),
    };

    setSavingScenario(true);

    try {
      await saveCloudScenario(user.uid, definition);

      setEditingScenarioId(definition.id);

      setSaveMessage(
        editingScenarioId
          ? `Updated "${definition.name}".`
          : `Saved "${definition.name}" to your account.`
      );

      // Remount the saved scenarios panel to reload Firestore.
      setSavedRevision((previous) => previous + 1);
    } catch (err) {
      setSaveError(
        err instanceof Error
          ? err.message
          : "Unable to save scenario."
      );
    } finally {
      setSavingScenario(false);
    }
  }

  function startNewScenario() {
    if (savingScenario) return;

    setEditingScenarioId(null);
    setScenarioName("");
    setStake(10);
    setBetType("win");
    setStakeAllocation("per_horse");
    setStakeMode("total");

    resetBuilderFilters();

    setSaveMessage(null);
    setSaveError(null);
    setBuilderOpen(true);
  }

  // Backtesting and comparison

  async function runComparison(
    from: string,
    to: string,
    override?: SelectedItem[]
  ) {
    const items = override ?? selectedItems;

    if (!items.length || !from || !to || from > to) {
      setError(
        "Select at least one scenario and a valid date range."
      );
      return;
    }

    const requestId = ++requestRef.current;

    setLoading(true);
    setError(null);

    const failures: string[] = [];
    const updated: SelectedItem[] = [];

    // Sequential requests avoid issuing many BigQuery jobs.

    for (const item of items) {
      if (requestId !== requestRef.current) return;

      try {
        const selection = item.selection;
        let response: Response;

        if (selection.source === "modelled") {
          const rank = Number(
            selection.id.split(":").at(-1)
          );

          if (
            !selection.runTimestamp ||
            !Number.isFinite(rank)
          ) {
            throw new Error(
              "Missing model strategy details."
            );
          }

          response = await fetch(
            "/api/scenarios/modelled/backtest",
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
                runTimestamp: selection.runTimestamp,
                rank,
                startDate: from,
                endDate: to,
              }),
            }
          );
        } else {
          const definition = item.definition;

          if (!definition) {
            throw new Error(
              "Missing scenario definition."
            );
          }

          response = await fetch(
            "/api/scenarios/calculate",
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
                startDate: from,
                endDate: to,
                stake: definition.stake,
                betType: definition.betType,
                stakeAllocation:
                  definition.stakeAllocation,
                stakeMode: definition.stakeMode,
                filters: definition.filters,
              }),
            }
          );
        }

        const json = await response.json();

        if (!response.ok) {
          throw new Error(
            json.error || "Backtest failed."
          );
        }

        const result: ScenarioResult = {
          summary: json.summary,
          dailyPerformance: json.dailyPerformance,
        };

        updated.push({
          ...item,
          selection: {
            ...selection,
            result,
            testedStartDate: from,
            testedEndDate: to,
          },
        });
      } catch (err) {
        failures.push(
          `${item.selection.name}: ${
            err instanceof Error
              ? err.message
              : "Backtest failed"
          }`
        );

        updated.push(item);
      }

      if (requestId === requestRef.current) {
        const completed = new Map(
          updated.map((item) => [
            item.selection.id,
            item,
          ])
        );

        setSelectedItems((previous) =>
          previous.map(
            (item) =>
              completed.get(item.selection.id) ?? item
          )
        );
      }
    }

    if (requestId === requestRef.current) {
      setLoading(false);

      setError(
        failures.length
          ? failures.join(" | ")
          : null
      );
    }
  }

  function selectModel(strategy: ModelStrategy) {
    toggleSelection({
      id: modelStrategyId(strategy),
      name: `Model strategy #${strategy.rank}`,
      source: "modelled",
      conditions: formatModelConditions(
        strategy.conditions
      ),
      startDate,
      endDate,
      runTimestamp: strategy.runTimestamp,
      result: null,
    });
  }

  function selectSaved(
    definition: ScenarioDefinition
  ) {
    toggleSelection(
      {
        id: `saved:${definition.id}`,
        name: definition.name,
        source: "saved",
        conditions: formatScenarioFilters(
          definition.filters
        ),
        startDate: definition.startDate,
        endDate: definition.endDate,
        result: null,
      },
      definition
    );
  }

  function loadSavedScenario(
    definition: ScenarioDefinition
  ) {
    const filters = definition.filters;

    // Preserve the ID for subsequent saves.
    setEditingScenarioId(definition.id);
    setBuilderOpen(true);

    setSaveMessage(null);
    setSaveError(null);

    setScenarioName(definition.name);
    setStartDate(definition.startDate);
    setEndDate(definition.endDate);
    setStake(definition.stake);
    setBetType(definition.betType);
    setStakeAllocation(definition.stakeAllocation);
    setStakeMode(definition.stakeMode);

    setMinOdds(
      filters.minOdds?.toString() ?? ""
    );

    setMaxOdds(
      filters.maxOdds?.toString() ?? ""
    );

    setMinPreRaceRank(
      filters.minPreRaceRank?.toString() ?? ""
    );

    setMaxPreRaceRank(
      filters.maxPreRaceRank?.toString() ?? ""
    );

    setFavouriteStatus(
      filters.favouriteStatus ?? "any"
    );

    setRaceTypes(
      filters.raceTypes ??
        (filters.raceType
          ? [filters.raceType]
          : [])
    );

    setCourses(
      filters.courses ??
        (filters.course
          ? [filters.course]
          : [])
    );

    setRaceClasses(
      filters.raceClasses ??
        (filters.raceClass
          ? [filters.raceClass]
          : [])
    );

    setGoings(
      filters.goings ??
        (filters.going
          ? [filters.going]
          : [])
    );

    setSurfaces(
      filters.surfaces ??
        (filters.surface
          ? [filters.surface]
          : [])
    );

    setMinDistance(
      filters.minDistance?.toString() ?? ""
    );

    setMaxDistance(
      filters.maxDistance?.toString() ?? ""
    );

    setHandicap(
      toBooleanChoice(filters.handicap)
    );

    setCourseWinner(
      toBooleanChoice(filters.courseWinner)
    );

    setDistanceWinner(
      toBooleanChoice(filters.distanceWinner)
    );

    setCourseDistanceWinner(
      toBooleanChoice(filters.courseDistanceWinner)
    );

    setBeatenFavourite(
      toBooleanChoice(filters.beatenFavourite)
    );

    setWonInLast3(
      toBooleanChoice(filters.wonInLast3)
    );

    setWin2InLast6(
      toBooleanChoice(filters.win2InLast6)
    );

    setEachWayEligible(
      toBooleanChoice(filters.eachWayEligible)
    );

    setError(null);
  }

  async function runBuilder() {
    if (
      !Number.isFinite(stake) ||
      stake <= 0 ||
      !startDate ||
      !endDate ||
      startDate > endDate
    ) {
      setError(
        "Enter a valid stake and date range."
      );
      return;
    }

    const definition = buildDefinition();

    const selection: PerformanceSelection = {
      id: `builder:${definition.id}`,
      name: definition.name,
      source: "builder",
      conditions: formatScenarioFilters(
        definition.filters
      ),
      startDate,
      endDate,
      result: null,
    };

    const item: SelectedItem = {
      selection,
      definition,
    };

    requestRef.current++;

    setSelectedItems((previous) => [
      ...previous,
      item,
    ]);

    setComparisonStart(startDate);
    setComparisonEnd(endDate);

    await runComparison(
      startDate,
      endDate,
      [...selectedItems, item]
    );
  }

  return (
    <main
      className="min-h-screen px-5 py-8 md:px-8"
      style={{
        background:
          "var(--racing-paper, #f7f4ed)",
        color: "var(--racing-ink, #202820)",
      }}
    >
      <div className="mx-auto max-w-7xl space-y-5">
        <header className="rounded-xl bg-[#003b2b] px-6 py-5 text-white shadow-sm">
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[#d7bf76]">
            Pacey32 Analytics
          </p>

          <h1 className="mt-1 font-serif text-3xl font-semibold md:text-4xl">
            Scenarios
          </h1>

          <p className="mt-1 text-sm text-white/75">
            Discover, save and backtest horse racing
            strategies.
          </p>
        </header>

        {/* Pacey32 modelled scenarios */}

        <section className="rounded-xl border border-[#ded8c9] bg-white p-4 shadow-sm">
          <button
            type="button"
            onClick={() =>
              setModelOpen(!modelOpen)
            }
            aria-expanded={modelOpen}
            className="flex w-full items-center justify-between text-left font-serif text-2xl text-[#123d2d]"
          >
            <span>
              Pacey32’s Top Scenarios
            </span>

            <span className="text-base">
              {modelOpen ? "−" : "+"}
            </span>
          </button>

          {modelOpen && (
            <div className="mt-3">
              <ModelledScenarios
                selectedIds={selectedItems
                  .filter(
                    (item) =>
                      item.selection.source ===
                      "modelled"
                  )
                  .map(
                    (item) => item.selection.id
                  )}
                onToggle={selectModel}
              />
            </div>
          )}
        </section>

        {/* Saved scenarios */}

        <section className="rounded-xl border border-[#ded8c9] bg-white p-4 shadow-sm">
          <button
            type="button"
            onClick={() =>
              setSavedOpen(!savedOpen)
            }
            aria-expanded={savedOpen}
            className="flex w-full items-center justify-between text-left font-serif text-2xl text-[#123d2d]"
          >
            <span>Saved Scenarios</span>

            <span className="text-base">
              {savedOpen ? "−" : "+"}
            </span>
          </button>

          {savedOpen && (
            <div className="mt-3">
              <SavedScenarios
                key={savedRevision}
                onLoadScenario={loadSavedScenario}
                onToggle={selectSaved}
                selectedIds={selectedItems
                  .filter(
                    (item) =>
                      item.selection.source ===
                      "saved"
                  )
                  .map((item) =>
                    item.selection.id.replace(
                      /^saved:/,
                      ""
                    )
                  )}
              />
            </div>
          )}
        </section>

        {/* Scenario Builder */}

        <section className="rounded-xl border border-[#ded8c9] bg-white p-4 shadow-sm">
          <button
            type="button"
            onClick={() =>
              setBuilderOpen(!builderOpen)
            }
            aria-expanded={builderOpen}
            className="flex w-full items-center justify-between text-left font-serif text-2xl text-[#123d2d]"
          >
            <span>Scenario Builder</span>

            <span className="text-base">
              {builderOpen ? "−" : "+"}
            </span>
          </button>

          {builderOpen && (
            <div className="mt-3 space-y-3">
              {/* Scenario settings */}

              <div className="grid grid-cols-2 items-end gap-2 md:grid-cols-12">
                <div className="col-span-2 md:col-span-3">
                  <Field label="Scenario name">
                    <input
                      value={scenarioName}
                      onChange={(e) =>
                        setScenarioName(e.target.value)
                      }
                      maxLength={80}
                      className="w-full rounded-md border border-gray-300 px-2 py-1.5 text-sm"
                    />
                  </Field>
                </div>

                <div className="md:col-span-2">
                  <Field label="Start date">
                    <input
                      type="date"
                      value={startDate}
                      onChange={(e) =>
                        setStartDate(e.target.value)
                      }
                      className="w-full min-w-0 rounded-md border border-gray-300 px-1.5 py-1.5 text-xs"
                    />
                  </Field>
                </div>

                <div className="md:col-span-2">
                  <Field label="End date">
                    <input
                      type="date"
                      value={endDate}
                      onChange={(e) =>
                        setEndDate(e.target.value)
                      }
                      className="w-full min-w-0 rounded-md border border-gray-300 px-1.5 py-1.5 text-xs"
                    />
                  </Field>
                </div>

                <div className="md:col-span-1">
                  <Field
                    label="Stake (£)"
                    help="Stake amount in pounds. For each-way bets, the stake mode determines whether this is total or per leg."
                  >
                    <input
                      type="number"
                      min="0.01"
                      step="0.5"
                      value={stake}
                      onChange={(e) =>
                        setStake(Number(e.target.value))
                      }
                      className="w-full min-w-0 rounded-md border border-gray-300 px-1.5 py-1.5 text-sm"
                    />
                  </Field>
                </div>

                <div className="md:col-span-2">
                  <Field label="Bet type">
                    <select
                      value={betType}
                      onChange={(e) =>
                        setBetType(
                          e.target.value as
                            | "win"
                            | "each_way"
                        )
                      }
                      className="w-full rounded-md border border-gray-300 px-1.5 py-1.5 text-sm"
                    >
                      <option value="win">
                        Win
                      </option>

                      <option value="each_way">
                        Each-way
                      </option>
                    </select>
                  </Field>
                </div>

                <div className="md:col-span-2">
                  <Field
                    label="Stake allocation"
                    help="Per horse applies the stake to every qualifier. Per race splits one stake between all qualifiers in that race."
                  >
                    <select
                      value={stakeAllocation}
                      onChange={(e) =>
                        setStakeAllocation(
                          e.target.value as
                            | "per_horse"
                            | "per_race"
                        )
                      }
                      className="w-full rounded-md border border-gray-300 px-1.5 py-1.5 text-sm"
                    >
                      <option value="per_horse">
                        Per horse
                      </option>

                      <option value="per_race">
                        Per race
                      </option>
                    </select>
                  </Field>
                </div>

                <div className="md:col-span-2">
                  <Field
                    label="Stake mode"
                    help="Total stake splits each-way stakes between win and place. Per leg stakes the entered amount on each leg."
                  >
                    <select
                      value={stakeMode}
                      onChange={(e) =>
                        setStakeMode(
                          e.target.value as
                            | "total"
                            | "per_leg"
                        )
                      }
                      disabled={betType === "win"}
                      className="w-full rounded-md border border-gray-300 px-1.5 py-1.5 text-sm disabled:opacity-50"
                    >
                      <option value="total">
                        Total stake
                      </option>

                      <option value="per_leg">
                        Per leg
                      </option>
                    </select>
                  </Field>
                </div>
              </div>

              {/* Betting conditions */}

              <div className="border-t border-gray-200 pt-2">
                <h3 className="mb-2 text-sm font-semibold text-[#123d2d]">
                  Betting conditions
                </h3>

                <div className="grid grid-cols-2 items-end gap-2 md:grid-cols-4">
                  <RangeField
                    title="Odds (decimal)"
                    min={minOdds}
                    max={maxOdds}
                    setMin={setMinOdds}
                    setMax={setMaxOdds}
                    step="0.1"
                    showFractional
                    hint="Enter minimum and maximum decimal odds. Fractional equivalents appear beside each value."
                  />

                  <RangeField
                    title="Horse odds rank"
                    min={minPreRaceRank}
                    max={maxPreRaceRank}
                    setMin={setMinPreRaceRank}
                    setMax={setMaxPreRaceRank}
                    hint="Rank 1 is the favourite (shortest odds)."
                  />

                  <div className="text-xs">
                    <p className="mb-1 font-semibold">
                      Favourite status
                      <Help text="Only includes favourites; Exclude removes favourites; Any ignores this condition." />
                    </p>

                    <div className="inline-flex rounded-md border border-gray-300 bg-gray-50 p-0.5">
                      {(
                        [
                          "not_favourite",
                          "any",
                          "favourite",
                        ] as const
                      ).map((value, index) => (
                        <button
                          key={value}
                          type="button"
                          aria-pressed={
                            favouriteStatus === value
                          }
                          onClick={() =>
                            setFavouriteStatus(value)
                          }
                          className={`rounded px-2 py-1 text-[11px] font-semibold ${
                            favouriteStatus === value
                              ? "bg-[#123d2d] text-white"
                              : "text-gray-600"
                          }`}
                        >
                          {
                            [
                              "Exclude",
                              "Any",
                              "Only",
                            ][index]
                          }
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              </div>

              {/* Race details */}

              <div className="border-t border-gray-200 pt-2">
                <h3 className="mb-2 text-sm font-semibold text-[#123d2d]">
                  Race details
                </h3>

                {optionsError && (
                  <p className="mb-1 text-xs text-amber-700">
                    Live filter options unavailable;
                    showing fallback choices.
                  </p>
                )}

                <div className="grid grid-cols-2 items-end gap-2 md:grid-cols-4 xl:grid-cols-7">
                  <MultiCheckbox
                    title="Race type"
                    options={["Flat", "Jumps"]}
                    selected={raceTypes}
                    onChange={setRaceTypes}
                  />

                  <MultiSelectDropdown
                    title="Course"
                    options={
                      filterOptions?.courses ??
                      COURSES
                    }
                    selected={courses}
                    onChange={setCourses}
                    loading={
                      !filterOptions &&
                      !optionsError
                    }
                  />

                  <MultiSelectDropdown
                    title="Going"
                    options={orderedGoings(
                      filterOptions?.goings ??
                        GOINGS
                    )}
                    selected={goings}
                    onChange={setGoings}
                    loading={
                      !filterOptions &&
                      !optionsError
                    }
                  />

                  <MultiSelectDropdown
                    title="Race class"
                    options={
                      filterOptions?.raceClasses ??
                      CLASSES
                    }
                    selected={raceClasses}
                    onChange={setRaceClasses}
                    loading={
                      !filterOptions &&
                      !optionsError
                    }
                  />

                  <MultiSelectDropdown
                    title="Surface"
                    options={
                      filterOptions?.surfaces ??
                      SURFACES
                    }
                    selected={surfaces}
                    onChange={setSurfaces}
                    loading={
                      !filterOptions &&
                      !optionsError
                    }
                  />

                  <RangeField
                    title="Distance (f)"
                    min={minDistance}
                    max={maxDistance}
                    setMin={setMinDistance}
                    setMax={setMaxDistance}
                    step="0.5"
                    hint="Race distance in furlongs. Leave blank for any distance."
                  />

                  <div className="text-xs">
                    <p className="mb-1 font-semibold">
                      Handicap
                      <Help text="Only includes handicap races; Exclude removes handicaps; Any includes both." />
                    </p>

                    <ChoiceSwitch
                      value={handicap}
                      onChange={setHandicap}
                    />
                  </div>
                </div>
              </div>

              {/* Horse history and recent form */}

              <div className="border-t border-gray-200 pt-2">
                <h3 className="mb-2 text-sm font-semibold text-[#123d2d]">
                  Horse history and recent form
                </h3>

                <div className="flex flex-wrap items-start gap-x-4 gap-y-2">
                  {(
                    [
                      [
                        "C",
                        "Course winner",
                        courseWinner,
                        setCourseWinner,
                      ],
                      [
                        "D",
                        "Distance winner",
                        distanceWinner,
                        setDistanceWinner,
                      ],
                      [
                        "CD",
                        "Course and distance winner",
                        courseDistanceWinner,
                        setCourseDistanceWinner,
                      ],
                      [
                        "BF",
                        "Beaten favourite",
                        beatenFavourite,
                        setBeatenFavourite,
                      ],
                      [
                        "Won last 3",
                        "Won at least once in the last three runs",
                        wonInLast3,
                        setWonInLast3,
                      ],
                      [
                        "Won 2 of last 6",
                        "Won at least twice in the last six runs",
                        win2InLast6,
                        setWin2InLast6,
                      ],
                      [
                        "Each-way",
                        "Eligible for each-way betting",
                        eachWayEligible,
                        setEachWayEligible,
                      ],
                    ] as Array<
                      [
                        string,
                        string,
                        BooleanChoice,
                        (value: BooleanChoice) => void,
                      ]
                    >
                  ).map(
                    ([
                      label,
                      help,
                      value,
                      setter,
                    ]) => (
                      <div
                        key={label}
                        className="min-w-0"
                      >
                        <p className="mb-1 text-xs font-semibold">
                          {label}
                          <Help text={help} />
                        </p>

                        <ChoiceSwitch
                          value={value}
                          onChange={setter}
                        />
                      </div>
                    )
                  )}
                </div>
              </div>

              {/* Builder actions */}

              <div className="flex flex-wrap justify-end gap-2 border-t border-gray-200 pt-2">
                <button
                  type="button"
                  onClick={startNewScenario}
                  disabled={savingScenario}
                  className="rounded-md border border-gray-300 bg-white px-4 py-2 text-xs font-semibold text-gray-700 disabled:opacity-50"
                >
                  New Scenario
                </button>

                <button
                  type="button"
                  onClick={resetBuilderFilters}
                  disabled={savingScenario}
                  className="rounded-md border border-[#123d2d] bg-white px-4 py-2 text-xs font-semibold text-[#123d2d] hover:bg-[#f8f7f2] disabled:opacity-50"
                >
                  Reset Filters
                </button>

                <button
                  type="button"
                  onClick={() =>
                    void handleSaveScenario()
                  }
                  disabled={
                    savingScenario ||
                    !currentUser ||
                    !scenarioName.trim() ||
                    !startDate ||
                    !endDate ||
                    startDate > endDate ||
                    !Number.isFinite(stake) ||
                    stake <= 0
                  }
                  className="rounded-md bg-[#123d2d] px-4 py-2 text-xs font-semibold text-white disabled:opacity-50"
                >
                  {savingScenario
                    ? "Saving…"
                    : "Save Scenario"}
                </button>

                <button
                  type="button"
                  onClick={() =>
                    void runBuilder()
                  }
                  disabled={
                    loading ||
                    !startDate ||
                    !endDate ||
                    startDate > endDate ||
                    stake <= 0
                  }
                  className="rounded-md bg-[#003b2b] px-4 py-2 text-xs font-semibold text-white disabled:opacity-50"
                >
                  {loading
                    ? "Calculating…"
                    : "Run Builder Backtest"}
                </button>
              </div>

              {editingScenarioId && (
                <p className="text-xs text-gray-500">
                  Editing an existing saved scenario.
                  Save Scenario will update it.
                  Select New Scenario to create
                  a separate scenario.
                </p>
              )}

              {saveMessage && (
                <p
                  role="status"
                  className="text-sm text-green-700"
                >
                  {saveMessage}
                </p>
              )}

              {saveError && (
                <p
                  role="alert"
                  className="text-sm text-red-700"
                >
                  {saveError}
                </p>
              )}

              {!currentUser && (
                <p className="text-xs text-gray-500">
                  Sign in to save scenarios
                  to your account.
                </p>
              )}
            </div>
          )}
        </section>

        {/* Comparison performance */}

        <ScenarioPerformance
          selections={selectedItems.map(
            (item) => item.selection
          )}
          loading={loading}
          error={error}
          startDate={comparisonStart}
          endDate={comparisonEnd}
          onDateChange={(from, to) => {
            setComparisonStart(from);
            setComparisonEnd(to);
          }}
          onBacktest={(from, to) =>
            runComparison(from, to)
          }
          onRemoveScenario={removeScenario}
        />

        {/* Individual scenario selections */}

        <ScenarioSelections
          scenario={
            selectedItems.length === 1 &&
            selectedItems[0].selection.source !==
              "modelled"
              ? selectedItems[0].definition
              : null
          }
          summary={
            selectedItems.length === 1
              ? selectedItems[0].selection.result
                  ?.summary ?? null
              : null
          }
        />
      </div>
    </main>
  );
}
