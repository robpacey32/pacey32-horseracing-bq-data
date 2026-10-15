"use client";

import { useEffect, useState } from "react";

import ScenarioChart from "@/components/scenarios/ScenarioChart";
import ScenarioSelections from "@/components/scenarios/ScenarioSelections";
import SavedScenarios from "@/components/scenarios/SavedScenarios";

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

function BooleanSelect({
  value,
  onChange,
}: {
  value: BooleanChoice;
  onChange: (value: BooleanChoice) => void;
}) {
  return (
    <select
      value={value}
      onChange={(event) =>
        onChange(event.target.value as BooleanChoice)
      }
      className="w-full rounded-lg border border-gray-300 px-3 py-2"
    >
      <option value="any">Any</option>
      <option value="yes">Yes</option>
      <option value="no">No</option>
    </select>
  );
}

function FilterSection({
  title,
  children,
  defaultOpen = false,
}: {
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}) {
  return (
    <details
      open={defaultOpen ? true : undefined}
      className="rounded-lg border border-gray-200 bg-white"
    >
      <summary className="cursor-pointer px-4 py-3 font-semibold">
        {title}
      </summary>

      <div className="grid grid-cols-1 gap-5 border-t border-gray-200 p-4 sm:grid-cols-2 lg:grid-cols-4">
        {children}
      </div>
    </details>
  );
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

  const [raceType, setRaceType] = useState("Flat");
  const [course, setCourse] = useState("");
  const [raceClass, setRaceClass] = useState("");
  const [going, setGoing] = useState("");
  const [surface, setSurface] = useState("");

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
      if (raceType !== "All") {
        filters.raceType = raceType;
      }

      if (course.trim()) {
        filters.course = course.trim();
      }

      if (raceClass.trim()) {
        filters.raceClass = raceClass.trim();
      }

      if (going.trim()) {
        filters.going = going.trim();
      }

      if (surface.trim()) {
        filters.surface = surface.trim();
      }

      // Favourite status
      if (favouriteStatus !== "any") {
        filters.favouriteStatus = favouriteStatus;
      }

      // Boolean filters
      const booleanValues = {
        handicap: optionalBoolean(handicap),
        courseWinner: optionalBoolean(courseWinner),
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
    setRaceType(filters.raceType ?? "All");
    setCourse(filters.course ?? "");
    setRaceClass(filters.raceClass ?? "");
    setGoing(filters.going ?? "");
    setSurface(filters.surface ?? "");

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

        {/* Page heading */}
        <div className="mb-8">
          <p
            className="text-xs font-semibold uppercase tracking-[0.25em]"
            style={{
              color: "var(--racing-gold-dark, #9c792e)",
            }}
          >
            Pacey32 Analytics
          </p>

          <h1
            className="mt-2 font-serif text-4xl font-semibold"
            style={{
              color: "var(--racing-green-dark, #123d2d)",
            }}
          >
            Scenarios
          </h1>

          <p className="mt-3 text-sm text-gray-600">
            Build, evaluate and compare historical horse racing
            betting strategies.
          </p>
        </div>

        {/* --------------------------------------------------
            Scenario Builder
        -------------------------------------------------- */}

        <section
          className="mb-7 rounded-xl border bg-white p-6 shadow-sm"
          style={{
            borderColor: "var(--racing-border, #ded8c9)",
          }}
        >
          <div className="mb-5 flex items-center justify-between">
            <h2
              className="font-serif text-2xl"
              style={{
                color: "var(--racing-green-dark, #123d2d)",
              }}
            >
              Scenario Builder
            </h2>

            <span className="text-sm text-gray-500">
              {comparedScenarios.length} / 4 scenarios
            </span>
          </div>

          {/* Core settings */}

          <div className="mb-6 grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">

            <Field label="Scenario name">
              <input
                value={scenarioName}
                onChange={(e) =>
                  setScenarioName(e.target.value)
                }
                maxLength={80}
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </Field>

            <Field label="Start date">
              <input
                type="date"
                value={startDate}
                onChange={(e) =>
                  setStartDate(e.target.value)
                }
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </Field>

            <Field label="End date">
              <input
                type="date"
                value={endDate}
                onChange={(e) =>
                  setEndDate(e.target.value)
                }
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </Field>

            <Field label="Stake">
              <input
                type="number"
                min="0.01"
                step="0.5"
                value={stake}
                onChange={(e) =>
                  setStake(Number(e.target.value))
                }
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </Field>

            <Field label="Bet type">
              <select
                value={betType}
                onChange={(e) =>
                  setBetType(
                    e.target.value as "win" | "each_way"
                  )
                }
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              >
                <option value="win">Win</option>
                <option value="each_way">Each-way</option>
              </select>
            </Field>

            <Field label="Stake allocation">
              <select
                value={stakeAllocation}
                onChange={(e) =>
                  setStakeAllocation(
                    e.target.value as
                      | "per_horse"
                      | "per_race"
                  )
                }
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              >
                <option value="per_horse">
                  Per horse
                </option>
                <option value="per_race">
                  Per race
                </option>
              </select>
            </Field>

            <Field label="Stake mode">
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
                className="w-full rounded-lg border border-gray-300 px-3 py-2 disabled:opacity-50"
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

          {/* --------------------------------------------------
              Filters
          -------------------------------------------------- */}

          <div className="space-y-3">

            {/* Betting Conditions */}

            <FilterSection
              title="Betting Conditions"
              defaultOpen
            >
              <Field label="Minimum odds">
                <input
                  type="number"
                  value={minOdds}
                  onChange={(e) =>
                    setMinOdds(e.target.value)
                  }
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Maximum odds">
                <input
                  type="number"
                  value={maxOdds}
                  onChange={(e) =>
                    setMaxOdds(e.target.value)
                  }
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Minimum odds rank">
                <input
                  type="number"
                  value={minPreRaceRank}
                  onChange={(e) =>
                    setMinPreRaceRank(e.target.value)
                  }
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Maximum odds rank">
                <input
                  type="number"
                  value={maxPreRaceRank}
                  onChange={(e) =>
                    setMaxPreRaceRank(e.target.value)
                  }
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Favourite status">
                <select
                  value={favouriteStatus}
                  onChange={(e) =>
                    setFavouriteStatus(
                      e.target.value as
                        | "any"
                        | "favourite"
                        | "not_favourite"
                    )
                  }
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                >
                  <option value="any">
                    Any
                  </option>
                  <option value="favourite">
                    Favourite only
                  </option>
                  <option value="not_favourite">
                    Exclude favourites
                  </option>
                </select>
              </Field>
            </FilterSection>

            {/* Race Details */}

            <FilterSection title="Race Details">

              <Field label="Race type">
                <select
                  value={raceType}
                  onChange={(e) =>
                    setRaceType(e.target.value)
                  }
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                >
                  <option value="All">All</option>
                  <option value="Flat">Flat</option>
                  <option value="Jumps">Jumps</option>
                  <option value="Unknown">
                    Unknown
                  </option>
                </select>
              </Field>

              <Field label="Course">
                <input
                  value={course}
                  onChange={(e) =>
                    setCourse(e.target.value)
                  }
                  placeholder="e.g. Ascot"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Race class">
                <input
                  value={raceClass}
                  onChange={(e) =>
                    setRaceClass(e.target.value)
                  }
                  placeholder="Exact class"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Going">
                <input
                  value={going}
                  onChange={(e) =>
                    setGoing(e.target.value)
                  }
                  placeholder="e.g. Good"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Surface">
                <input
                  value={surface}
                  onChange={(e) =>
                    setSurface(e.target.value)
                  }
                  placeholder="Exact surface"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Minimum distance (furlongs)">
                <input
                  type="number"
                  step="0.5"
                  value={minDistance}
                  onChange={(e) =>
                    setMinDistance(e.target.value)
                  }
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Maximum distance (furlongs)">
                <input
                  type="number"
                  step="0.5"
                  value={maxDistance}
                  onChange={(e) =>
                    setMaxDistance(e.target.value)
                  }
                  className="w-full rounded-lg border border-gray-300 px-3 py-2"
                />
              </Field>

              <Field label="Handicap race">
                <BooleanSelect
                  value={handicap}
                  onChange={setHandicap}
                />
              </Field>

            </FilterSection>

            {/* Horse History */}

            <FilterSection title="Horse History">

              <Field label="Previous course winner">
                <BooleanSelect
                  value={courseWinner}
                  onChange={setCourseWinner}
                />
              </Field>

              <Field label="Previous distance winner">
                <BooleanSelect
                  value={distanceWinner}
                  onChange={setDistanceWinner}
                />
              </Field>

              <Field label="Course & distance winner">
                <BooleanSelect
                  value={courseDistanceWinner}
                  onChange={setCourseDistanceWinner}
                />
              </Field>

            </FilterSection>

            {/* Recent Form */}

            <FilterSection title="Recent Form">

              <Field label="Won in last 3">
                <BooleanSelect
                  value={wonInLast3}
                  onChange={setWonInLast3}
                />
              </Field>

              <Field label="Won twice in last 6">
                <BooleanSelect
                  value={win2InLast6}
                  onChange={setWin2InLast6}
                />
              </Field>

            </FilterSection>

            {/* Each-Way */}

            <FilterSection title="Each-Way Conditions">

              <Field label="Each-way eligible">
                <BooleanSelect
                  value={eachWayEligible}
                  onChange={setEachWayEligible}
                />
              </Field>

            </FilterSection>

          </div>

          {/* Run button */}

          <div className="mt-6 flex justify-end">
            <button
              onClick={runScenario}
              disabled={
                loading ||
                !startDate ||
                !endDate ||
                comparedScenarios.length >= 4
              }
              className="rounded-lg px-6 py-3 text-sm font-semibold text-white transition hover:opacity-90 disabled:opacity-50"
              style={{
                background: "var(--racing-green, #164734)",
              }}
            >
              {loading
                ? "Calculating..."
                : comparedScenarios.length >= 4
                  ? "Comparison Full (4/4)"
                  : "Run & Compare"}
            </button>
          </div>

          {error && (
            <p className="mt-4 text-sm text-red-700">
              {error}
            </p>
          )}
        </section>

        {/* --------------------------------------------------
            Saved Scenarios - V2.3
        -------------------------------------------------- */}

        <SavedScenarios
          selectedScenario={
            selectedScenario?.definition ?? null
          }
          onLoadScenario={loadSavedScenario}
        />

        {/* --------------------------------------------------
            Chart
        -------------------------------------------------- */}

        <ScenarioChart
          scenarios={comparedScenarios}
        />

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