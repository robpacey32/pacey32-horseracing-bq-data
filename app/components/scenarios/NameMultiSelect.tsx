"use client";

import { useEffect, useId, useState } from "react";

interface Props {
  title: string;
  kind: "horse" | "jockey" | "trainer";
  selected: string[];
  onChange: (names: string[]) => void;
}

type SearchResult = {
  key: string;
  names: string[];
  hasMore: boolean;
  error: string | null;
};

const normalize = (name: string) => name.trim().toLowerCase();

export default function NameMultiSelect({ title, kind, selected, onChange }: Props) {
  const labelId = useId();
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [retry, setRetry] = useState(0);
  const [result, setResult] = useState<SearchResult | null>(null);
  const query = normalize(search);
  const requestKey = `${kind}:${query}`;
  const current = result?.key === requestKey ? result : null;
  const searching = open && query.length >= 2 && !current;

  useEffect(() => {
    if (!open || query.length < 2) return;

    const controller = new AbortController();
    const timer = setTimeout(async () => {
      setResult(null);

      try {
        const params = new URLSearchParams({ kind, q: query });
        const response = await fetch(`/api/scenarios/names?${params}`, {
          signal: controller.signal,
        });
        const body = await response.json();

        if (!response.ok) {
          throw new Error(body?.error ?? "Could not load names.");
        }

        if (!Array.isArray(body.names) || body.names.some((name: unknown) => typeof name !== "string")) {
          throw new Error("Could not load names.");
        }

        if (!controller.signal.aborted) {
          setResult({ key: requestKey, names: body.names, hasMore: body.hasMore === true, error: null });
        }
      } catch (error) {
        if (!controller.signal.aborted) {
          setResult({
            key: requestKey,
            names: [],
            hasMore: false,
            error: error instanceof Error ? error.message : "Could not load names.",
          });
        }
      }
    }, 350);

    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [open, query, kind, requestKey, retry]);

  function toggle(name: string) {
    const exists = selected.some((item) => normalize(item) === normalize(name));
    if (exists) {
      onChange(selected.filter((item) => normalize(item) !== normalize(name)));
    } else if (selected.length < 250) {
      onChange([...selected, name]);
    }
  }

  return (
    <div className="min-w-0 text-xs">
      <p id={labelId} className="mb-1 font-semibold">{title}</p>

      <details
        className="relative rounded-md border border-gray-300 bg-white"
        onToggle={(event) => setOpen(event.currentTarget.open)}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.currentTarget.open = false;
          }
        }}
      >
        <summary
          aria-labelledby={labelId}
          className="flex cursor-pointer list-none items-center justify-between gap-2 px-2 py-1.5 text-sm marker:hidden [&::-webkit-details-marker]:hidden"
        >
          <span className="truncate">
            {selected.length === 0 ? "All" : selected.length === 1 ? selected[0] : `${selected.length} selected`}
          </span>
          <span aria-hidden="true" className="text-gray-500">▾</span>
        </summary>

        {open && (
          <div className="absolute left-0 top-full z-40 mt-1 w-full min-w-[260px] rounded-md border border-gray-300 bg-white p-3 shadow-xl">
            <input
              type="search"
              aria-label={`Search ${title.toLowerCase()}`}
              placeholder="Type at least 2 characters…"
              maxLength={100}
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              className="w-full rounded border border-gray-300 px-2 py-1.5 text-sm"
            />

            <div className="my-2 flex items-center justify-between gap-2 text-xs">
              <span>{selected.length} selected</span>
              <button type="button" onClick={() => onChange([])} className="text-[#123d2d] underline">
                Clear selections
              </button>
            </div>

            {selected.length > 0 && (
              <div className="mb-2 max-h-28 overflow-y-auto border-b border-gray-200 pb-2">
                <p className="mb-1 font-medium text-gray-600">Selected</p>
                {selected.map((name) => (
                  <label key={name} className="flex cursor-pointer items-center gap-2 py-1">
                    <input type="checkbox" checked onChange={() => toggle(name)} className="accent-[#123d2d]" />
                    {name}
                  </label>
                ))}
              </div>
            )}

            <div aria-live="polite" className="text-xs text-gray-500">
              {query.length < 2 && "Type at least 2 characters to search names."}
              {searching && "Searching…"}
              {current?.error && (
                <p role="alert">
                  {current.error}{" "}
                  <button type="button" onClick={() => setRetry((value) => value + 1)} className="text-[#123d2d] underline">
                    Try again
                  </button>
                </p>
              )}
              {current && !current.error && current.names.length === 0 && "No matching names found."}
            </div>

            {current && !current.error && (
              <div className="mt-2 max-h-48 overflow-y-auto">
                {current.names.filter((name) => !selected.some((item) => normalize(item) === normalize(name))).map((name) => (
                  <label key={name} className="flex cursor-pointer items-center gap-2 rounded px-1 py-1.5 hover:bg-gray-50">
                    <input
                      type="checkbox"
                      checked={false}
                      disabled={selected.length >= 250}
                      onChange={() => toggle(name)}
                      className="accent-[#123d2d]"
                    />
                    {name}
                  </label>
                ))}
                {current.hasMore && <p className="mt-2 text-gray-500">Showing 50 matches. Type more to narrow the search.</p>}
              </div>
            )}

            {selected.length >= 250 && <p className="mt-2 text-amber-700">You can select up to 250 names.</p>}
            <p className="mt-2 border-t border-gray-100 pt-2 text-gray-500">
              Choose exact names. Leave unselected to include all.
            </p>
          </div>
        )}
      </details>
    </div>
  );
}
