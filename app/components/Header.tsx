
"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import { auth } from "@/lib/firebase";

const navigation = [
  {
    name: "Race Viewer",
    href: "/race-viewer",
    symbol: "♞",
  },
  {
    name: "Scenarios",
    href: "/scenarios",
    symbol: "▥",
  },
  {
    name: "Bet Tracker",
    href: "/bet-tracker",
    symbol: "▤",
  },
  // QA page remains accessible directly at /qa
];

export default function Header() {
  const pathname = usePathname();

  const [menuOpen, setMenuOpen] = useState(false);
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);

  const menuRef = useRef<HTMLDivElement>(null);

  // Listen for Firebase authentication changes
  useEffect(() => {
    return onAuthStateChanged(auth, (currentUser) => {
      setUser(currentUser);
      setAuthLoading(false);
    });
  }, []);

  // Close menu when clicking outside
  useEffect(() => {
    function handleOutsideClick(event: MouseEvent) {
      if (
        menuRef.current &&
        !menuRef.current.contains(event.target as Node)
      ) {
        setMenuOpen(false);
      }
    }

    document.addEventListener("mousedown", handleOutsideClick);

    return () => {
      document.removeEventListener("mousedown", handleOutsideClick);
    };
  }, []);

  // Close menu when navigating
  useEffect(() => {
    setMenuOpen(false);
  }, [pathname]);

  const accountLabel = authLoading
    ? "Account"
    : user
      ? "My Account"
      : "Sign in";

  const menuItems = [
    ...navigation,
    {
      name: accountLabel,
      href: "/account",
      symbol: "♙",
    },
  ];

  return (
    <header className="relative z-50 border-b border-[var(--racing-gold)] bg-[var(--racing-paper)]">
      <div className="flex h-[68px] w-full items-center justify-between px-5 md:px-8 lg:px-10">

        {/* Brand */}
        <Link
          href="/"
          className="flex h-full items-center"
          aria-label="Pacey32 Racing Home"
        >
          <Image
            src="/branding/pacey32-racing-logo.png"
            alt="Pacey32 Racing"
            width={420}
            height={120}
            priority
            className="h-[54px] w-auto object-contain"
          />
        </Link>

        {/* Menu */}
        <div ref={menuRef} className="relative">
          <button
            type="button"
            aria-expanded={menuOpen}
            aria-haspopup="menu"
            onClick={() => setMenuOpen((current) => !current)}
            className="racing-serif flex h-10 items-center gap-3 rounded border border-[var(--racing-gold)] bg-[var(--racing-green)] px-5 text-[16px] font-medium text-white transition hover:bg-[var(--racing-green-dark)]"
          >
            Menu

            <span
              className={`text-[10px] transition-transform ${
                menuOpen ? "rotate-180" : ""
              }`}
            >
              ▾
            </span>
          </button>

          {menuOpen && (
            <div
              role="menu"
              className="absolute right-0 top-[48px] w-56 overflow-hidden rounded border border-[var(--racing-border)] bg-[var(--racing-paper)] shadow-xl"
            >
              {menuItems.map((item) => {
                const active =
                  pathname === item.href ||
                  pathname.startsWith(`${item.href}/`);

                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    role="menuitem"
                    onClick={() => setMenuOpen(false)}
                    className={`flex items-center gap-4 border-b border-[var(--racing-border)] px-5 py-3.5 last:border-b-0 ${
                      active
                        ? "bg-[var(--racing-green-light)] text-[var(--racing-green-dark)]"
                        : "text-[var(--racing-ink)] hover:bg-[var(--racing-ivory)]"
                    }`}
                  >
                    <span className="w-5 text-center text-base">
                      {item.symbol}
                    </span>

                    <span className="racing-serif text-[17px] font-medium">
                      {item.name}
                    </span>
                  </Link>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
