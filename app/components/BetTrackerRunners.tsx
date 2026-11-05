"use client";



import { useEffect, useMemo, useState } from "react";

import { addDoc, collection, serverTimestamp } from "firebase/firestore";

import { auth, db } from "@/lib/firebase";



interface Runner {

  race_id: string;

  runner_id: string;

  RaceDate: string;

  RaceTime: string;

  RaceLocation: string;

  RaceName: string;

  HorseNumber: number | string | null;

  HorseName: string;

  Trainer: string | null;

  Jockey: string | null;

  Odds: string | null;

  Form: string | null;

  RaceClass: string | null;

  RaceDistance: string | null;

  RaceGoing: string | null;

  RaceSurface: string | null;
  RaceType: string | null;
  Age: string | null;
  Weight: string | null;
  RaceHistoryStats: string | null;
  LastRun: string | null;
  Headgear: string | null;

}



type RunnerSort = "odds-asc" | "odds-desc" | "number-asc" | "number-desc" | "name-asc" | "name-desc";



function oddsValue(odds: string | null): number {

  if (!odds) return Number.NaN;

  const value = odds.trim().toUpperCase();

  if (value === "EVS" || value === "EVENS" || value === "EVEN") return 2;

  const fractional = value.match(/^(\d+(?:\.\d+)?)\s*\/\s*(\d+(?:\.\d+)?)$/);
  if (fractional && Number(fractional[2]) > 0) {

    return 1 + Number(fractional[1]) / Number(fractional[2]);

  }

  const decimal = Number(value);

  return Number.isFinite(decimal) && decimal > 1 ? decimal : Number.NaN;

}



function sortedRunners(entries: Runner[], sort: RunnerSort, query: string): Runner[] {

  const matching = entries.filter((runner) =>

    runner.HorseName.toLowerCase().includes(query.trim().toLowerCase())

  );

  return [...matching].sort((a, b) => {

    if (sort.startsWith("name")) {

      const result = a.HorseName.localeCompare(b.HorseName);

      return (sort === "name-asc" ? result : -result) || a.runner_id.localeCompare(b.runner_id);

    }

    if (sort.startsWith("number")) {

      const first = a.HorseNumber == null || String(a.HorseNumber).trim() === "" ? Number.NaN : Number(a.HorseNumber);

      const second = b.HorseNumber == null || String(b.HorseNumber).trim() === "" ? Number.NaN : Number(b.HorseNumber);

      if (!Number.isFinite(first) && !Number.isFinite(second)) return a.HorseName.localeCompare(b.HorseName);

      if (!Number.isFinite(first)) return 1;

      if (!Number.isFinite(second)) return -1;

      return ((sort === "number-asc" ? first - second : second - first) || a.HorseName.localeCompare(b.HorseName));

    }

    const first = oddsValue(a.Odds);

    const second = oddsValue(b.Odds);

    if (!Number.isFinite(first) && !Number.isFinite(second)) return a.HorseName.localeCompare(b.HorseName);

    if (!Number.isFinite(first)) return 1;

    if (!Number.isFinite(second)) return -1;

    return ((sort === "odds-asc" ? first - second : second - first) || a.HorseName.localeCompare(b.HorseName));

  });

}



export default function BetTrackerRunners() {

  const [date, setDate] = useState(

    () => new Date().toLocaleDateString("en-CA")

  );



  const [runners, setRunners] = useState<Runner[]>([]);

  const [loading, setLoading] = useState(false);

  const [error, setError] = useState("");



  const [course, setCourse] = useState("");

  const [search, setSearch] = useState("");
  const [jockeySearch, setJockeySearch] = useState("");
  const [trainerSearch, setTrainerSearch] = useState("");
  const [raceType, setRaceType] = useState("");

  const [expanded, setExpanded] = useState<string[]>([]);

  const [expandedCourses, setExpandedCourses] = useState<string[]>([]);

  const [raceSorts, setRaceSorts] = useState<Record<string, RunnerSort>>({});

  const [selectedRunner, setSelectedRunner] = useState<Runner | null>(null);

  const [betType, setBetType] = useState<"WIN" | "EW">("WIN");

  const [stake, setStake] = useState("");

  const [takenOdds, setTakenOdds] = useState("");

  const [formError, setFormError] = useState("");

  const [saving, setSaving] = useState(false);

  const [saveMessage, setSaveMessage] = useState("");



  useEffect(() => {

    const controller = new AbortController();



    async function loadRunners() {

      setLoading(true);

      setError("");

      setRunners([]);

      setExpanded([]);

      setExpandedCourses([]);

      setRaceSorts({});

      setSelectedRunner(null);



      try {

        const response = await fetch(

          `/api/bet-tracker/runners?date=${date}`,

          { signal: controller.signal }

        );



        if (!response.ok) {

          throw new Error("Unable to retrieve runners.");

        }



        const data = await response.json();

        setRunners(data.runners ?? []);

      } catch (err) {

        if (controller.signal.aborted) return;



        setError(

          err instanceof Error

            ? err.message

            : "Unable to retrieve runners."

        );

      } finally {

        if (!controller.signal.aborted) {

          setLoading(false);

        }

      }

    }



    loadRunners();



    return () => controller.abort();

  }, [date]);



  const courses = useMemo(

    () =>

      [...new Set(runners.map((r) => r.RaceLocation))]

        .sort(),

    [runners]

  );



  const filtered = useMemo(
    () => runners.filter((runner) =>
      (!course || runner.RaceLocation === course) &&
      (!raceType || runner.RaceType === raceType) &&
      runner.HorseName.toLowerCase().includes(search.trim().toLowerCase()) &&
      (runner.Jockey ?? "").toLowerCase().includes(jockeySearch.trim().toLowerCase()) &&
      (runner.Trainer ?? "").toLowerCase().includes(trainerSearch.trim().toLowerCase())
    ),
    [runners, course, raceType, search, jockeySearch, trainerSearch]
  );

  const hasSearch = Boolean(search.trim() || jockeySearch.trim() || trainerSearch.trim());

  const races = useMemo(() => {

    const groups = new Map<string, Runner[]>();



    for (const runner of filtered) {

      const key = runner.race_id;



      if (!groups.has(key)) {

        groups.set(key, []);

      }



      groups.get(key)!.push(runner);

    }



    return [...groups.entries()].sort((a, b) => {

      const first = a[1][0];

      const second = b[1][0];



      return (

        first.RaceTime.localeCompare(second.RaceTime) ||

        first.RaceLocation.localeCompare(second.RaceLocation)

      );

    });

  }, [filtered]);



  const groupedCourses = useMemo(() => {

    const groups = new Map<string, { races: [string, Runner[]][]; runnerCount: number }>();



    for (const [raceId, entries] of races) {

      const location = entries[0].RaceLocation;

      if (!groups.has(location)) {

        groups.set(location, { races: [], runnerCount: 0 });

      }

      const group = groups.get(location)!;

      group.races.push([raceId, entries]);

      group.runnerCount += entries.length;

    }



    return [...groups.entries()].sort((a, b) => a[0].localeCompare(b[0]));

  }, [races]);



  function toggleCourse(location: string) {

    setExpandedCourses((current) =>

      current.includes(location)

        ? current.filter((item) => item !== location)

        : [...current, location]

    );

  }



  function toggleRace(raceId: string) {

    setExpanded((current) =>

      current.includes(raceId)

        ? current.filter((id) => id !== raceId)

        : [...current, raceId]

    );

  }



  function selectRunner(runner: Runner) {

    setSelectedRunner(runner);

    setBetType("WIN");

    setStake("");

    setTakenOdds(runner.Odds ?? "");

    setFormError("");

    setSaveMessage("");

  }



  async function saveBet(event: React.FormEvent<HTMLFormElement>) {

    event.preventDefault();

    if (!selectedRunner || saving) return;



    const user = auth.currentUser;

    if (!user) {

      setFormError("Your session has expired. Sign in again to save this bet.");

      return;

    }



    // Validate the bet before writing it to Firestore.

    const amount = Number(stake);

    const odds = takenOdds.trim();

    const match = odds.match(/^(\d+(?:\.\d+)?)\s*\/\s*(\d+(?:\.\d+)?)$/);
    const validOdds =

      Boolean(match && Number(match[2]) > 0) ||

      odds.toUpperCase() === "EVS" ||

      (/^\d+(?:\.\d+)?$/.test(odds) && Number(odds) > 1);



    if (!Number.isFinite(amount) || amount <= 0 || !validOdds) {

      setFormError("Enter a valid stake and odds before saving.");

      return;

    }



    setSaving(true);

    setFormError("");

    try {

      await addDoc(collection(db, "users", user.uid, "bets"), {

        raceId: selectedRunner.race_id,

        runnerId: selectedRunner.runner_id,

        raceDate: selectedRunner.RaceDate,

        raceTime: selectedRunner.RaceTime,

        raceLocation: selectedRunner.RaceLocation,

        raceName: selectedRunner.RaceName,

        horseNumber: selectedRunner.HorseNumber ?? null,

        horseName: selectedRunner.HorseName,

        trainer: selectedRunner.Trainer ?? null,

        jockey: selectedRunner.Jockey ?? null,

        form: selectedRunner.Form ?? null,

        originalOdds: selectedRunner.Odds ?? null,

        takenOdds: odds,

        betType,

        stake: amount,

        createdAt: serverTimestamp(),

      });

      setSaveMessage(`Bet saved successfully: ${selectedRunner.HorseName}.`);

    } catch (err) {

      console.error("Failed to save bet:", err);

      setFormError("Unable to save this bet. Please try again.");

    } finally {

      setSaving(false);

    }

  }



  return (

    <div className="mt-8">

      {/* Filters */}

      <div className="rounded-xl border border-gray-200 bg-white p-5">

        <div className="grid gap-4 md:grid-cols-3">

          <label className="text-sm font-medium text-gray-700">

            Race date

            <input

              type="date"

              value={date}

              onChange={(e) => {

                setDate(e.target.value);

                setCourse("");

              }}

              className="mt-2 w-full rounded-lg border border-gray-300 px-3 py-2"

            />

          </label>



          <label className="text-sm font-medium text-gray-700">

            Racecourse

            <select

              value={course}

              onChange={(e) => setCourse(e.target.value)}

              className="mt-2 w-full rounded-lg border border-gray-300 px-3 py-2"

            >

              <option value="">All racecourses</option>



              {courses.map((location) => (

                <option key={location} value={location}>

                  {location}

                </option>

              ))}

            </select>

          </label>



          <label className="text-sm font-medium text-gray-700">
          Race type
          <select value={raceType} onChange={(e) => setRaceType(e.target.value)} className="mt-2 w-full rounded-lg border border-gray-300 px-3 py-2">
            <option value="">All race types</option>
            {["Flat", "Hurdle", "Chase", "NH Flat", "Unknown"].map((type) => (
              <option key={type} value={type}>{type}</option>
            ))}
          </select>
        </label>
        <label className="text-sm font-medium text-gray-700">
          Horse search
          <input type="search" value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search horses..." className="mt-2 w-full rounded-lg border border-gray-300 px-3 py-2" />
        </label>
        <label className="text-sm font-medium text-gray-700">
          Jockey search
          <input type="search" value={jockeySearch} onChange={(e) => setJockeySearch(e.target.value)} placeholder="Search jockeys..." className="mt-2 w-full rounded-lg border border-gray-300 px-3 py-2" />
        </label>
        <label className="text-sm font-medium text-gray-700">
          Trainer search
          <input type="search" value={trainerSearch} onChange={(e) => setTrainerSearch(e.target.value)} placeholder="Search trainers..." className="mt-2 w-full rounded-lg border border-gray-300 px-3 py-2" />
        </label>
      </div>

      </div>



      {/* Summary */}

      <div className="mt-6 flex items-center justify-between">

        <h2 className="text-xl font-semibold text-gray-900">

          Available Runners

        </h2>



        <span className="text-sm text-gray-500">

          {filtered.length} runners · {races.length} races

        </span>

      </div>



      {loading && (

        <p className="mt-5 text-gray-500">

          Loading runners...

        </p>

      )}



      {error && (

        <p className="mt-5 text-red-600">{error}</p>

      )}



      {!loading && !error && races.length === 0 && (

        <p className="mt-5 text-gray-500">

          No runners found for the selected filters.

        </p>

      )}



      {/* Racecourses → Races → Runners */}

      <div className="mt-4 space-y-4">

        {groupedCourses.map(([location, group]) => {

          const courseExpanded = hasSearch || expandedCourses.includes(location);



          return (

            <div

              key={location}

              className="overflow-hidden rounded-xl border border-gray-300 bg-white"

            >

              <button

                type="button"

                onClick={() => toggleCourse(location)}

                aria-expanded={courseExpanded}

                className="flex w-full items-center justify-between gap-4 bg-gray-100 px-5 py-4 text-left hover:bg-gray-200"

              >

                <h3 className="text-lg font-semibold text-gray-900">{location}</h3>

                <div className="flex items-center gap-4">

                  <span className="text-sm text-gray-600">

                    {group.races.length} races · {group.runnerCount} runners

                  </span>

                  <span className="text-xl text-gray-600">

                    {courseExpanded ? "−" : "+"}

                  </span>

                </div>

              </button>



              {courseExpanded && (

                <div className="space-y-3 p-4">

                  {group.races.map(([raceId, entries]) => {

                    const race = entries[0];

                    const raceExpanded = hasSearch || expanded.includes(raceId);

                    const raceSort = raceSorts[raceId] ?? "odds-asc";

                    const displayedEntries = sortedRunners(entries, raceSort, "");



                    return (

                      <div

                        key={raceId}

                        className="overflow-hidden rounded-lg border border-gray-200"

                      >

                        <button

                          type="button"

                          onClick={() => toggleRace(raceId)}

                          aria-expanded={raceExpanded}

                          className="flex w-full items-center justify-between gap-4 p-4 text-left hover:bg-gray-50"

                        >

                          <div>

                            <h4 className="font-semibold text-gray-900">

                              {race.RaceTime} — {race.RaceName}

                            </h4>

                            <p className="mt-1 text-xs text-gray-500">

                              {[race.RaceDistance, race.RaceGoing, race.RaceSurface]

                                .filter(Boolean)

                                .join(" · ")}

                            </p>

                          </div>

                          <div className="flex shrink-0 items-center gap-3">

                            <span className="text-sm text-gray-500">

                              {entries.length} runners

                            </span>

                            <span className="text-xl text-gray-500">

                              {raceExpanded ? "−" : "+"}

                            </span>

                          </div>

                        </button>



                        {raceExpanded && (

                          <div className="border-t border-gray-200">

                            <div className="overflow-x-auto">

                            <table className="min-w-max w-full text-left text-sm">

                              <thead className="bg-gray-50 text-gray-600">

                                <tr>

                                  <th className="px-4 py-3"><button type="button" onClick={() => setRaceSorts((current) => ({ ...current, [raceId]: raceSort === "number-asc" ? "number-desc" : "number-asc" }))} aria-label="Sort by horse number" className="font-semibold hover:text-gray-900">No. {raceSort === "number-asc" ? "▲" : raceSort === "number-desc" ? "▼" : "↕"}</button></th>

                                  <th className="px-4 py-3"><button type="button" onClick={() => setRaceSorts((current) => ({ ...current, [raceId]: raceSort === "name-asc" ? "name-desc" : "name-asc" }))} aria-label="Sort by horse name" className="font-semibold hover:text-gray-900">Horse {raceSort === "name-asc" ? "▲" : raceSort === "name-desc" ? "▼" : "↕"}</button></th>

                                  <th className="px-4 py-3">Form</th>
                                  <th className="px-4 py-3">Age</th>
                                  <th className="px-4 py-3">Weight</th>
                                  <th className="px-4 py-3">Jockey</th>
                                  <th className="px-4 py-3">Trainer</th>
                                  <th className="px-4 py-3"><button type="button" onClick={() => setRaceSorts((current) => ({ ...current, [raceId]: raceSort === "odds-asc" ? "odds-desc" : "odds-asc" }))} aria-label="Sort by odds" className="font-semibold hover:text-gray-900">Odds {raceSort === "odds-asc" ? "▲" : raceSort === "odds-desc" ? "▼" : "↕"}</button></th>

                                  <th className="px-4 py-3">Race history</th>
                                 <th className="px-4 py-3">Last run</th>
                                 <th className="px-4 py-3">Headgear</th>
                                 <th className="px-4 py-3">Bet</th>

                                </tr>

                              </thead>

                              <tbody>

                                {displayedEntries.map((runner) => (

                                  <tr key={runner.runner_id} className="border-t border-gray-100">

                                    <td className="px-4 py-3 text-gray-600">

                                      {runner.HorseNumber ?? "—"}

                                    </td>

                                    <td className="px-4 py-3 font-medium text-gray-900">

                                      {runner.HorseName}

                                    </td>

                                    <td className="px-4 py-3 text-gray-600">{runner.Form || "—"}</td>
                                   <td className="px-4 py-3 text-gray-600">{runner.Age || "—"}</td>
                                   <td className="px-4 py-3 text-gray-600">{runner.Weight || "—"}</td>
                                   <td className="px-4 py-3 text-gray-600">{runner.Jockey || "—"}</td>
                                   <td className="px-4 py-3 text-gray-600">{runner.Trainer || "—"}</td>
                                   <td className="px-4 py-3 font-medium text-gray-900">

                                      {runner.Odds ?? "—"}

                                    </td>

                                    <td className="px-4 py-3 text-gray-600">{runner.RaceHistoryStats || "—"}</td>
                                   <td className="px-4 py-3 text-gray-600">{runner.LastRun || "—"}</td>
                                   <td className="px-4 py-3 text-gray-600">{runner.Headgear || "—"}</td>
                                   <td className="px-4 py-3">

                                      <button

                                        type="button"

                                        onClick={() => selectRunner(runner)}

                                        className="rounded-lg border border-amber-600 px-3 py-1.5 font-semibold text-amber-800 hover:bg-amber-50"

                                      >

                                        Select

                                      </button>

                                    </td>

                                  </tr>

                                ))}

                              </tbody>

                            </table>

                            {displayedEntries.length === 0 && (

                              <p className="px-4 py-4 text-sm text-gray-500">No matching horses.</p>

                            )}

                            </div>

                          </div>

                        )}

                      </div>

                    );

                  })}

                </div>

              )}

            </div>

          );

        })}

      </div>



      {selectedRunner && (

        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" onMouseDown={(event) => {

          if (event.target === event.currentTarget && !saving) setSelectedRunner(null);

        }}>

          <div role="dialog" aria-modal="true" aria-labelledby="bet-form-title" className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl">

            <div className="flex items-start justify-between gap-4">

              <div>

                <h3 id="bet-form-title" className="text-xl font-semibold text-gray-900">Record a bet</h3>

                <p className="mt-1 font-medium text-gray-800">{selectedRunner.HorseName}</p>

                <p className="text-sm text-gray-500">{selectedRunner.RaceDate} · {selectedRunner.RaceTime} · {selectedRunner.RaceLocation}</p>

              </div>

              <button type="button" onClick={() => { if (!saving) setSelectedRunner(null); }} aria-label="Close bet form" className="rounded-lg px-2 py-1 text-xl text-gray-500 hover:bg-gray-100">×</button>

            </div>



            <form onSubmit={saveBet} className="mt-6 space-y-4">

              <label className="block text-sm font-medium text-gray-700">

                Bet type

                <select value={betType} onChange={(event) => setBetType(event.target.value as "WIN" | "EW")} disabled={Boolean(saveMessage) || saving} className="mt-2 w-full rounded-lg border border-gray-300 px-3 py-2">

                  <option value="WIN">Win</option>

                  <option value="EW">Each Way</option>

                </select>

              </label>

              <label className="block text-sm font-medium text-gray-700">

                Stake (£)

                <input type="number" min="0.01" step="0.01" inputMode="decimal" value={stake} onChange={(event) => setStake(event.target.value)} disabled={Boolean(saveMessage) || saving} placeholder="e.g. 5.00" className="mt-2 w-full rounded-lg border border-gray-300 px-3 py-2" required />

              </label>

              <label className="block text-sm font-medium text-gray-700">

                Odds taken

                <input type="text" value={takenOdds} onChange={(event) => setTakenOdds(event.target.value)} disabled={Boolean(saveMessage) || saving} placeholder="e.g. 7/2" className="mt-2 w-full rounded-lg border border-gray-300 px-3 py-2" required />

              </label>

              {formError && <p role="alert" className="text-sm text-red-600">{formError}</p>}

              {saveMessage && <p role="status" className="rounded-lg border border-green-200 bg-green-50 p-3 text-sm text-green-800">{saveMessage}</p>}

              <div className="flex justify-end gap-3 pt-2">

                {saveMessage ? (

                  <button type="button" onClick={() => setSelectedRunner(null)} className="rounded-lg bg-amber-700 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-800">Close</button>

                ) : (

                  <>

                    <button type="button" onClick={() => setSelectedRunner(null)} disabled={saving} className="rounded-lg border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50">Cancel</button>

                    <button type="submit" disabled={saving} className="rounded-lg bg-amber-700 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-800 disabled:cursor-not-allowed disabled:opacity-50">{saving ? "Saving..." : "Save bet"}</button>

                  </>

                )}

              </div>

            </form>

          </div>

        </div>

      )}

    </div>

  );

}
