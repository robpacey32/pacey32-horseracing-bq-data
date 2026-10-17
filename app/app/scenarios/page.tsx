"use client";

import { useEffect, useState } from "react";

import ScenarioChart from "@/components/scenarios/ScenarioChart";
import ScenarioSelections from "@/components/scenarios/ScenarioSelections";
import SavedScenarios from "@/components/scenarios/SavedScenarios";
import ModelledScenarios from "@/components/scenarios/ModelledScenarios";

import type {
  ComparedScenario,
  ScenarioDefinition,
  ScenarioFilters,
  ScenarioResult,
} from "@/lib/scenarios/types";

type BooleanChoice = "any" | "yes" | "no";

function formatNumber(value: number, decimals = 2) {
  return value.toLocaleString("en-GB", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

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

function MetricCard({
  label,
  value,
  negative = false,
}: {
  label: string;
  value: string;
  negative?: boolean;
}) {
  return (
    <div
      className="rounded-xl border p-5"
      style={{
        background: "var(--racing-ivory, #fffdf7)",
        borderColor: "var(--racing-border, #ded8c9)",
      }}
    >
      <div className="text-xs font-semibold uppercase tracking-widest text-gray-500">
        {label}
      </div>

      <div
        className={`mt-3 text-2xl font-semibold ${
          negative ? "text-red-700" : ""
        }`}
        style={
          negative
            ? undefined
            : { color: "var(--racing-green, #164734)" }
        }
      >
        {value}
      </div>
    </div>
  );
}

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label className="text-sm">
      <span className="mb-2 block font-medium">{label}</span>
      {children}
    </label>
  );
}

function ChoiceSwitch({ value, onChange, labels = ["Include", "Only", "Exclude"] }: {
  value: BooleanChoice;
  onChange: (value: BooleanChoice) => void;
  labels?: [string, string, string];
}) {
  return (
    <div className="inline-flex max-w-full rounded-lg border border-gray-300 bg-gray-50 p-1">
      {(["any", "yes", "no"] as const).map((option, index) => (
        <button key={option} type="button" onClick={() => onChange(option)}
          aria-pressed={value === option}
          className={`rounded-md px-3 py-1.5 text-xs font-semibold transition ${value === option ? "bg-[#123d2d] text-white" : "text-gray-600 hover:bg-white"}`}>
          {labels[index]}
        </button>
      ))}
    </div>
  );
}

function FilterSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <details className="rounded-lg border border-gray-200 bg-white">
      <summary className="cursor-pointer px-4 py-3 font-semibold">{title}</summary>
      <div className="grid grid-cols-1 gap-4 border-t border-gray-200 p-4 md:grid-cols-2 xl:grid-cols-3">{children}</div>
    </details>
  );
}

function RangeField({ title, min, max, setMin, setMax, step = "1", hint }: {
  title: string; min: string; max: string;
  setMin: (value: string) => void; setMax: (value: string) => void;
  step?: string; hint?: string;
}) {
  return (
    <div className="text-sm">
      <div className="mb-2 font-medium">{title}</div>
      <div className="grid grid-cols-2 gap-2">
        <label><span className="mb-1 block text-xs text-gray-500">Min</span><input type="number" step={step} value={min} onChange={e => setMin(e.target.value)} className="w-full rounded-lg border border-gray-300 px-3 py-2" /></label>
        <label><span className="mb-1 block text-xs text-gray-500">Max</span><input type="number" step={step} value={max} onChange={e => setMax(e.target.value)} className="w-full rounded-lg border border-gray-300 px-3 py-2" /></label>
      </div>
      {hint && <p className="mt-1 text-xs text-gray-500">{hint}</p>}
    </div>
  );
}

function MultiSelectDropdown({ title, options, selected, onChange, loading = false }: {
  title: string;
  options: string[];
  selected: string[];
  onChange: (values: string[]) => void;
  loading?: boolean;
}) {
  const [search, setSearch] = useState("");
  const values = Array.from(new Set([...options, ...selected]));
  const visible = values.filter(value => value.toLocaleLowerCase().includes(search.trim().toLocaleLowerCase()));
  const toggle = (value: string) => onChange(selected.includes(value)
    ? selected.filter(item => item !== value)
    : [...selected, value]);
  return (
    <div className="min-w-0 text-sm">
      <p className="mb-2 font-medium">{title}</p>
      <details className="group relative rounded-lg border border-gray-300 bg-white">
        <summary className="flex cursor-pointer list-none items-center justify-between gap-2 px-3 py-2.5 marker:hidden [&::-webkit-details-marker]:hidden">
          <span className="truncate">{selected.length === 0 ? "All" : `${selected.length} selected`}</span>
          <span className="text-gray-500 group-open:rotate-180">▾</span>
        </summary>
        <div className="absolute left-0 right-0 top-full z-30 mt-1 rounded-lg border border-gray-300 bg-white p-3 shadow-xl">
          <input type="search" value={search} onChange={event => setSearch(event.target.value)}
            placeholder={`Search ${title.toLowerCase()}…`} aria-label={`Search ${title}`}
            className="mb-2 w-full rounded-md border border-gray-300 px-2 py-1.5" />
          <div className="mb-2 flex justify-between text-xs">
            <button type="button" onClick={() => onChange(values)} className="font-medium text-[#123d2d] hover:underline">Select all</button>
            <button type="button" onClick={() => onChange([])} className="text-gray-600 hover:underline">Clear all</button>
          </div>
          <div className="max-h-56 space-y-1 overflow-y-auto">
            {visible.map(value => <label key={value} className="flex cursor-pointer items-center gap-2 rounded px-1 py-1 hover:bg-gray-50">
              <input type="checkbox" checked={selected.includes(value)} onChange={() => toggle(value)} className="accent-[#123d2d]" />
              <span>{value}</span>
            </label>)}
            {!visible.length && <p className="text-xs text-gray-500">{loading ? "Loading options…" : "No matching values"}</p>}
          </div>
        </div>
      </details>
      <p className="mt-1 text-xs text-gray-500">None selected = all included</p>
    </div>
  );
}

function MultiCheckbox({title, options, selected, onChange}: {title:string; options:string[]; selected:string[]; onChange:(items:string[])=>void}) {
  return <div className="text-sm"><p className="mb-2 font-medium">{title}</p>
    <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 grid grid-cols-2 gap-2">
      {options.map(option => <label key={option} className="flex items-center gap-2 text-xs cursor-pointer">
        <input type="checkbox" checked={selected.includes(option)} onChange={e => onChange(e.target.checked ? [...selected,option] : selected.filter(v=>v!==option))} className="accent-[#123d2d]" />{option}
      </label>)}
    </div><p className="mt-1 text-xs text-gray-500">None selected = all included</p>
  </div>;
}

const COURSES = ["Aintree", "Ascot", "Ayr", "Bangor", "Bath", "Beverley", "Brighton", "Carlisle", "Cartmel", "Catterick", "Chelmsford", "Cheltenham", "Chepstow", "Chester", "Clonmel", "Curragh", "Doncaster", "Dundalk", "Epsom", "Exeter", "Fairyhouse", "Ffos Las", "Fontwell", "Galway", "Goodwood", "Hamilton", "Haydock", "Hexham", "Huntingdon", "Kempton", "Leicester", "Leopardstown", "Limerick", "Lingfield", "Listowel", "Ludlow", "Market Rasen", "Musselburgh", "Naas", "Navan", "Newbury", "Newcastle", "Newmarket", "Nottingham", "Perth", "Pontefract", "Punchestown", "Redcar", "Ripon", "Roscommon", "Salisbury", "Sandown", "Sedgefield", "Southwell", "Stratford", "Taunton", "Thirsk", "Thurles", "Tipperary", "Uttoxeter", "Warwick", "Wetherby", "Wincanton", "Windsor", "Wolverhampton", "Worcester", "Yarmouth", "York"];
const GOINGS = ["Heavy", "Soft To Heavy", "Soft", "Yielding To Soft", "Yielding", "Good To Soft", "Good", "Good To Firm", "Firm", "Fast", "Standard To Slow", "Standard"];
const CLASSES = ["Class 1", "Class 2", "Class 3", "Class 4", "Class 5", "Class 6", "Class 7"];
const SURFACES = ["Turf", "All Weather", "All-Weather", "AW", "Tapeta", "Polytrack", "Fibresand", "Dirt", "Grass"];

function orderedGoings(values: string[]): string[] {
  const order = GOINGS.map(value => value.toLowerCase());
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
  // --------------------------------------------------
  // Dates
  // --------------------------------------------------

  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");

  useEffect(() => {
    const today = new Date();
    const start = new Date(today);

    start.setMonth(start.getMonth() - 3);

    setStartDate(start.toISOString().slice(0, 10));
    setEndDate(today.toISOString().slice(0, 10));
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    fetch("/api/scenarios/options", { signal: controller.signal })
      .then(async response => {
        if (!response.ok) throw new Error("Could not load filter options");
        return response.json();
      })
      .then(data => setFilterOptions(data))
      .catch(error => { if (error.name !== "AbortError") setOptionsError(true); });
    return () => controller.abort();
  }, []);

  // --------------------------------------------------
  // Scenario settings
  // --------------------------------------------------

  const [scenarioName, setScenarioName] = useState("Scenario 1");

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

  // --------------------------------------------------
  // Betting filters
  // --------------------------------------------------

  const [minOdds, setMinOdds] = useState("2");
  const [maxOdds, setMaxOdds] = useState("10");

  const [minPreRaceRank, setMinPreRaceRank] = useState("");
  const [maxPreRaceRank, setMaxPreRaceRank] = useState("3");

  const [favouriteStatus, setFavouriteStatus] = useState<
    "any" | "favourite" | "not_favourite"
  >("any");

  // --------------------------------------------------
  // Race filters
  // --------------------------------------------------

  const [raceTypes, setRaceTypes] = useState<string[]>(["Flat"]);
  const [courses, setCourses] = useState<string[]>([]);
  const [raceClasses, setRaceClasses] = useState<string[]>([]);
  const [filterOptions, setFilterOptions] = useState<{ courses: string[]; raceClasses: string[]; goings: string[]; surfaces: string[] } | null>(null);
  const [optionsError, setOptionsError] = useState(false);
  const [goings, setGoings] = useState<string[]>([]);
  const [surfaces, setSurfaces] = useState<string[]>([]);

  const [minDistance, setMinDistance] = useState("");
  const [maxDistance, setMaxDistance] = useState("");

  const [handicap, setHandicap] =
    useState<BooleanChoice>("any");

  // --------------------------------------------------
  // Horse history
  // --------------------------------------------------

  const [courseWinner, setCourseWinner] =
    useState<BooleanChoice>("any");

  const [distanceWinner, setDistanceWinner] =
    useState<BooleanChoice>("any");

  const [courseDistanceWinner, setCourseDistanceWinner] =
    useState<BooleanChoice>("any");

  const [beatenFavourite, setBeatenFavourite] = useState<BooleanChoice>("any");

  // --------------------------------------------------
  // Recent form
  // --------------------------------------------------

  const [wonInLast3, setWonInLast3] =
    useState<BooleanChoice>("any");

  const [win2InLast6, setWin2InLast6] =
    useState<BooleanChoice>("any");

  // --------------------------------------------------
  // Each-way
  // --------------------------------------------------

  const [eachWayEligible, setEachWayEligible] =
    useState<BooleanChoice>("any");

  // --------------------------------------------------
  // Results and comparison
  // --------------------------------------------------

  const [result, setResult] =
    useState<ScenarioResult | null>(null);

  const [comparedScenarios, setComparedScenarios] = useState<
    ComparedScenario[]
  >([]);

  const [selectedScenarioId, setSelectedScenarioId] = useState<
    string | null
  >(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // --------------------------------------------------
  // Selected scenario
  // --------------------------------------------------

  const selectedScenario =
    comparedScenarios.find(
      (scenario) =>
        scenario.definition.id === selectedScenarioId
    ) ?? null;

  // --------------------------------------------------
  // Run scenario
  // --------------------------------------------------

  async function runScenario() {
    if (comparedScenarios.length >= 4) {
      setError("Remove a scenario before adding another.");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const filters: ScenarioFilters = {};

      // Numeric filters
      const numericValues = {
        minOdds: optionalNumber(minOdds),
        maxOdds: optionalNumber(maxOdds),
        minPreRaceRank: optionalNumber(minPreRaceRank),
        maxPreRaceRank: optionalNumber(maxPreRaceRank),
        minDistance: optionalNumber(minDistance),
        maxDistance: optionalNumber(maxDistance),
      };

      for (const [key, value] of Object.entries(numericValues)) {
        if (value !== undefined) {
          Object.assign(filters, { [key]: value });
        }
      }

      // Text filters
      if (raceTypes.length) filters.raceTypes = raceTypes;

      if (courses.length) filters.courses = courses;
      if (raceClasses.length) filters.raceClasses = raceClasses;

      if (goings.length) filters.goings = goings;

      if (surfaces.length) filters.surfaces = surfaces;

      // Favourite status
      if (favouriteStatus !== "any") {
        filters.favouriteStatus = favouriteStatus;
      }

      // Boolean filters
      const booleanValues = {
        handicap: optionalBoolean(handicap),
        courseWinner: optionalBoolean(courseWinner),
        beatenFavourite: optionalBoolean(beatenFavourite),
        distanceWinner: optionalBoolean(distanceWinner),
        courseDistanceWinner: optionalBoolean(
          courseDistanceWinner
        ),
        wonInLast3: optionalBoolean(wonInLast3),
        win2InLast6: optionalBoolean(win2InLast6),
        eachWayEligible: optionalBoolean(eachWayEligible),
      };

      for (const [key, value] of Object.entries(booleanValues)) {
        if (value !== undefined) {
          Object.assign(filters, { [key]: value });
        }
      }

      const definition: ScenarioDefinition = {
        id: crypto.randomUUID(),
        name:
          scenarioName.trim() ||
          `Scenario ${comparedScenarios.length + 1}`,
        startDate,
        endDate,
        stake,
        betType,
        stakeAllocation,
        stakeMode,
        filters,
      };

      const response = await fetch(
        "/api/scenarios/calculate",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            startDate,
            endDate,
            stake,
            betType,
            stakeAllocation,
            stakeMode,
            filters,
          }),
        }
      );

      const data = await response.json();

      if (!response.ok) {
        throw new Error(
          data.error || "Scenario calculation failed."
        );
      }

      const scenarioResult = data as ScenarioResult;

      setResult(scenarioResult);

      setComparedScenarios((previous) => [
        ...previous,
        {
          definition,
          result: scenarioResult,
        },
      ]);

      setSelectedScenarioId(definition.id);

      setScenarioName(
        `Scenario ${comparedScenarios.length + 2}`
      );
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "An unexpected error occurred."
      );
    } finally {
      setLoading(false);
    }
  }

  // --------------------------------------------------
  // Remove scenario
  // --------------------------------------------------

  function removeScenario(id: string) {
    const remaining = comparedScenarios.filter(
      (scenario) => scenario.definition.id !== id
    );

    setComparedScenarios(remaining);

    if (selectedScenarioId === id) {
      const replacement = remaining.at(-1);

      setSelectedScenarioId(
        replacement?.definition.id ?? null
      );

      setResult(replacement?.result ?? null);
    }
  }

  // --------------------------------------------------
  // Select scenario
  // --------------------------------------------------

  function selectScenario(id: string) {
    const scenario = comparedScenarios.find(
      (item) => item.definition.id === id
    );

    if (!scenario) return;

    setSelectedScenarioId(id);
    setResult(scenario.result);
  }

  // --------------------------------------------------
  // Load saved scenario into builder
  // --------------------------------------------------

  function loadSavedScenario(
    definition: ScenarioDefinition
  ) {
    const filters = definition.filters;

    // Core settings
    setScenarioName(definition.name);
    setStartDate(definition.startDate);
    setEndDate(definition.endDate);

    setStake(definition.stake);
    setBetType(definition.betType);
    setStakeAllocation(definition.stakeAllocation);
    setStakeMode(definition.stakeMode);

    // Betting filters
    setMinOdds(filters.minOdds?.toString() ?? "");
    setMaxOdds(filters.maxOdds?.toString() ?? "");

    setMinPreRaceRank(
      filters.minPreRaceRank?.toString() ?? ""
    );

    setMaxPreRaceRank(
      filters.maxPreRaceRank?.toString() ?? ""
    );

    setFavouriteStatus(
      filters.favouriteStatus ?? "any"
    );

    // Race filters
    setRaceTypes(filters.raceTypes ?? (filters.raceType ? [filters.raceType] : []));
    setCourses(filters.courses ?? (filters.course ? [filters.course] : []));
    setRaceClasses(filters.raceClasses ?? (filters.raceClass ? [filters.raceClass] : []));
    setGoings(filters.goings ?? (filters.going ? [filters.going] : []));
    setSurfaces(filters.surfaces ?? (filters.surface ? [filters.surface] : []));

    setMinDistance(
      filters.minDistance?.toString() ?? ""
    );

    setMaxDistance(
      filters.maxDistance?.toString() ?? ""
    );

    // Horse history and race conditions
    setHandicap(toBooleanChoice(filters.handicap));

    setCourseWinner(
      toBooleanChoice(filters.courseWinner)
    );
    setBeatenFavourite(toBooleanChoice(filters.beatenFavourite));

    setDistanceWinner(
      toBooleanChoice(filters.distanceWinner)
    );

    setCourseDistanceWinner(
      toBooleanChoice(filters.courseDistanceWinner)
    );

    // Recent form
    setWonInLast3(
      toBooleanChoice(filters.wonInLast3)
    );

    setWin2InLast6(
      toBooleanChoice(filters.win2InLast6)
    );

    // Each-way
    setEachWayEligible(
      toBooleanChoice(filters.eachWayEligible)
    );

    setError(null);
  }

  // --------------------------------------------------
  // Render
  // --------------------------------------------------

  return (
    <main
      className="min-h-screen px-5 py-8 md:px-8"
      style={{
        background: "var(--racing-paper, #f7f4ed)",
        color: "var(--racing-ink, #202820)",
      }}
    >
      <div className="mx-auto max-w-7xl">

        <header className="mb-5 rounded-xl bg-[#003b2b] px-6 py-5 text-white shadow-sm">
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[#d7bf76]">Pacey32 Analytics</p>
          <h1 className="mt-1 font-serif text-3xl font-semibold md:text-4xl">Scenarios</h1>
          <p className="mt-1 text-sm text-white/75">Build, evaluate and compare horse racing strategies.</p>
        </header>

        <details className="mb-5 overflow-hidden rounded-xl border border-[#ded8c9] bg-white shadow-sm">
          <summary className="cursor-pointer bg-[#f8f7f2] px-5 py-4 font-serif text-2xl text-[#123d2d]">
            Scenario Builder <span className="float-right pt-2 font-sans text-xs text-gray-500">{comparedScenarios.length} / 4 compared</span>
          </summary>
          <div className="space-y-5 border-t border-[#ded8c9] p-5">
            <Field label="Scenario name">
              <input value={scenarioName} onChange={e => setScenarioName(e.target.value)} maxLength={80} className="w-full rounded-lg border border-gray-300 px-3 py-2" />
            </Field>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
              <Field label="Start date"><input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} className="w-full rounded-lg border border-gray-300 px-2 py-2" /></Field>
              <Field label="End date"><input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} className="w-full rounded-lg border border-gray-300 px-2 py-2" /></Field>
              <Field label="Stake (£)"><input type="number" min="0.01" step="0.5" value={stake} onChange={e => setStake(Number(e.target.value))} className="w-full rounded-lg border border-gray-300 px-3 py-2" /></Field>
              <Field label="Bet type"><select value={betType} onChange={e => setBetType(e.target.value as "win" | "each_way")} className="w-full rounded-lg border border-gray-300 px-3 py-2"><option value="win">Win</option><option value="each_way">Each-way</option></select></Field>
              <Field label="Stake distribution"><select value={stakeAllocation} onChange={e => setStakeAllocation(e.target.value as "per_horse" | "per_race")} className="w-full rounded-lg border border-gray-300 px-2 py-2"><option value="per_horse">Full stake per horse</option><option value="per_race">Split stake across horses</option></select></Field>
              <Field label="Stake mode"><select value={stakeMode} onChange={e => setStakeMode(e.target.value as "total" | "per_leg")} disabled={betType === "win"} className="w-full rounded-lg border border-gray-300 px-2 py-2 disabled:opacity-50"><option value="total">Total stake</option><option value="per_leg">Per leg</option></select></Field>
            </div>
            <p className="text-xs text-gray-500">Stake distribution: full stake per qualifying horse, or one total race stake shared between all qualifying horses. Neither option restricts selections to one horse per race.</p>
            <div className="space-y-2">
              <FilterSection title="Betting Conditions">
                <RangeField title="Odds (decimal)" min={minOdds} max={maxOdds} setMin={setMinOdds} setMax={setMaxOdds} step="0.1" />
                <RangeField title="Horse odds rank" min={minPreRaceRank} max={maxPreRaceRank} setMin={setMinPreRaceRank} setMax={setMaxPreRaceRank} hint="Rank 1 = shortest-priced horse (favourite)." />
                <div><p className="mb-2 text-sm font-medium">Favourites</p><div className="inline-flex rounded-lg border border-gray-300 bg-gray-50 p-1">{(["any", "favourite", "not_favourite"] as const).map((v, i) => <button key={v} type="button" aria-pressed={favouriteStatus === v} onClick={() => setFavouriteStatus(v)} className={`rounded-md px-3 py-1.5 text-xs font-semibold ${favouriteStatus === v ? "bg-[#123d2d] text-white" : "text-gray-600"}`}>{["Any", "Only", "Exclude"][i]}</button>)}</div></div>
              </FilterSection>
              {optionsError && <p className="text-xs text-amber-700">Could not load live filter options; showing fallback choices.</p>}
              <FilterSection title="Race Details">
                <MultiCheckbox title="Race type" options={["Flat", "Jumps"]} selected={raceTypes} onChange={setRaceTypes} />
                <MultiSelectDropdown title="Course" options={filterOptions?.courses ?? COURSES} selected={courses} onChange={setCourses} loading={!filterOptions && !optionsError} />
                <MultiSelectDropdown title="Going" options={orderedGoings(filterOptions?.goings ?? GOINGS)} selected={goings} onChange={setGoings} loading={!filterOptions && !optionsError} />
                <MultiSelectDropdown title="Race class" options={filterOptions?.raceClasses ?? CLASSES} selected={raceClasses} onChange={setRaceClasses} loading={!filterOptions && !optionsError} />
                <MultiSelectDropdown title="Surface" options={filterOptions?.surfaces ?? SURFACES} selected={surfaces} onChange={setSurfaces} loading={!filterOptions && !optionsError} />
                <RangeField title="Distance (furlongs)" min={minDistance} max={maxDistance} setMin={setMinDistance} setMax={setMaxDistance} step="0.5" />
                <div><p className="mb-2 text-sm font-medium">Handicap race</p><ChoiceSwitch value={handicap} onChange={setHandicap} /></div>
              </FilterSection>
              <FilterSection title="Horse History">
                <div><p className="mb-2 text-sm font-medium">C — Course winner</p><ChoiceSwitch value={courseWinner} onChange={setCourseWinner} /></div>
                <div><p className="mb-2 text-sm font-medium">D — Distance winner</p><ChoiceSwitch value={distanceWinner} onChange={setDistanceWinner} /></div>
                <div><p className="mb-2 text-sm font-medium">CD — Course &amp; distance winner</p><ChoiceSwitch value={courseDistanceWinner} onChange={setCourseDistanceWinner} /></div>
                <div><p className="mb-2 text-sm font-medium">BF — Beaten favourite</p><ChoiceSwitch value={beatenFavourite} onChange={setBeatenFavourite} /></div>
              </FilterSection>
              <FilterSection title="Recent Form">
                <div><p className="mb-2 text-sm font-medium">Won in last 3</p><ChoiceSwitch value={wonInLast3} onChange={setWonInLast3} /></div>
                <div><p className="mb-2 text-sm font-medium">Won twice in last 6</p><ChoiceSwitch value={win2InLast6} onChange={setWin2InLast6} /></div>
              </FilterSection>
            </div>
            <div className="flex justify-end"><button type="button" onClick={runScenario} disabled={loading || !startDate || !endDate || comparedScenarios.length >= 4} className="rounded-lg bg-[#003b2b] px-6 py-3 text-sm font-semibold text-white disabled:opacity-50">{loading ? "Calculating…" : comparedScenarios.length >= 4 ? "Comparison Full (4/4)" : "Run & Compare"}</button></div>
            {error && <p className="text-sm text-red-700">{error}</p>}
            <div className="border-t border-gray-200 pt-5"><ScenarioChart scenarios={comparedScenarios} /></div>
          </div>
        </details>

        <details className="mb-5 overflow-hidden rounded-xl border border-[#ded8c9] bg-white shadow-sm">
          <summary className="cursor-pointer bg-[#f8f7f2] px-5 py-4 font-serif text-2xl text-[#123d2d]">Saved Scenarios</summary>
          <div className="border-t border-[#ded8c9] p-4">
            <SavedScenarios selectedScenario={selectedScenario?.definition ?? null} onLoadScenario={loadSavedScenario} />
            <p className="mt-2 text-xs text-gray-500">Saved definitions do not include performance data. Load a scenario, then run it to calculate its chart.</p>
            {selectedScenario && <div className="mt-4 border-t border-gray-200 pt-4"><ScenarioChart scenarios={[selectedScenario]} /></div>}
          </div>
        </details>

        <details className="mb-5 overflow-hidden rounded-xl border border-[#ded8c9] bg-white shadow-sm">
          <summary className="cursor-pointer bg-[#f8f7f2] px-5 py-4 font-serif text-2xl text-[#123d2d]">Modelled Scenarios</summary>
          <div className="border-t border-[#ded8c9] p-4"><ModelledScenarios /></div>
        </details>

        {/* --------------------------------------------------
            Comparison Manager
        -------------------------------------------------- */}

        {comparedScenarios.length > 0 && (
          <section className="mb-7 rounded-xl border bg-white p-6">

            <div className="mb-4 flex items-center justify-between">
              <h2 className="font-serif text-2xl">
                Scenario Comparison
              </h2>

              <span className="text-sm text-gray-500">
                {comparedScenarios.length} / 4 scenarios
              </span>
            </div>

            <div className="space-y-3">

              {comparedScenarios.map((scenario) => {
                const selected =
                  scenario.definition.id ===
                  selectedScenarioId;

                return (
                  <div
                    key={scenario.definition.id}
                    className={`flex flex-wrap items-center justify-between gap-4 rounded-lg border p-4 ${
                      selected
                        ? "border-amber-400 bg-amber-50"
                        : "border-gray-200"
                    }`}
                  >
                    <div>
                      <div className="font-semibold">
                        {scenario.definition.name}
                      </div>

                      <div className="mt-1 text-sm text-gray-500">
                        {formatNumber(
                          scenario.result.summary.bets,
                          0
                        )}{" "}
                        bets ·{" "}
                        {formatNumber(
                          scenario.result.summary.roi
                        )}
                        % ROI
                      </div>
                    </div>

                    <div className="flex gap-2">

                      <button
                        onClick={() =>
                          selectScenario(
                            scenario.definition.id
                          )
                        }
                        className="rounded-lg border border-gray-300 px-3 py-2 text-sm"
                      >
                        {selected
                          ? "Selected"
                          : "View Summary"}
                      </button>

                      <button
                        onClick={() =>
                          removeScenario(
                            scenario.definition.id
                          )
                        }
                        className="rounded-lg border border-red-200 px-3 py-2 text-sm text-red-700"
                      >
                        Remove
                      </button>

                    </div>
                  </div>
                );
              })}

            </div>
          </section>
        )}

        {/* --------------------------------------------------
            Performance Summary
        -------------------------------------------------- */}

        {result && (
          <section className="mb-7">

            <h2 className="mb-4 font-serif text-2xl">
              Performance Summary
            </h2>

            <div className="grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-6">

              <MetricCard
                label="Bets"
                value={formatNumber(
                  result.summary.bets,
                  0
                )}
              />

              <MetricCard
                label="Strike Rate"
                value={`${formatNumber(
                  result.summary.strikeRate
                )}%`}
              />

              <MetricCard
                label="Staked"
                value={formatNumber(
                  result.summary.staked
                )}
              />

              <MetricCard
                label="Profit"
                value={formatNumber(
                  result.summary.profit
                )}
                negative={
                  result.summary.profit < 0
                }
              />

              <MetricCard
                label="ROI"
                value={`${formatNumber(
                  result.summary.roi
                )}%`}
                negative={
                  result.summary.roi < 0
                }
              />

              <MetricCard
                label="Max Drawdown"
                value={formatNumber(
                  result.summary.maxDrawdown
                )}
              />

            </div>
          </section>
        )}

        {/* --------------------------------------------------
            Historical Selections - V2.2b
        -------------------------------------------------- */}

        <ScenarioSelections
          scenario={
            selectedScenario?.definition ?? null
          }
          summary={
            result?.summary ?? null
          }
        />

        {/* --------------------------------------------------
            Comparison Metrics
        -------------------------------------------------- */}

        {comparedScenarios.length > 1 && (
          <section className="mb-7 overflow-hidden rounded-xl border bg-white">

            <div className="border-b border-gray-200 p-6">
              <h2 className="font-serif text-2xl">
                Comparison Metrics
              </h2>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full min-w-[650px] text-sm">

                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-5 py-4 text-left">
                      Metric
                    </th>

                    {comparedScenarios.map(
                      (scenario) => (
                        <th
                          key={
                            scenario.definition.id
                          }
                          className="px-5 py-4 text-right"
                        >
                          {
                            scenario.definition.name
                          }
                        </th>
                      )
                    )}
                  </tr>
                </thead>

                <tbody className="divide-y divide-gray-100">

                  {(
                    [
                      ["Bets", "bets"],
                      ["Winners", "winners"],
                      ["Strike Rate (%)", "strikeRate"],
                      ["Staked", "staked"],
                      ["Returns", "returns"],
                      ["Profit", "profit"],
                      ["ROI (%)", "roi"],
                      ["Max Drawdown", "maxDrawdown"],
                    ] as const
                  ).map(([label, key]) => (
                    <tr key={key}>

                      <td className="px-5 py-3 font-medium">
                        {label}
                      </td>

                      {comparedScenarios.map(
                        (scenario) => {
                          const value =
                            scenario.result.summary[
                              key
                            ];

                          const negative =
                            (key === "profit" ||
                              key === "roi") &&
                            value < 0;

                          return (
                            <td
                              key={
                                scenario.definition.id
                              }
                              className={`px-5 py-3 text-right ${
                                negative
                                  ? "font-semibold text-red-700"
                                  : ""
                              }`}
                            >
                              {formatNumber(
                                value,
                                key === "bets" ||
                                  key === "winners"
                                  ? 0
                                  : 2
                              )}
                            </td>
                          );
                        }
                      )}

                    </tr>
                  ))}

                </tbody>
              </table>
            </div>
          </section>
        )}

      </div>
    </main>
  );
}