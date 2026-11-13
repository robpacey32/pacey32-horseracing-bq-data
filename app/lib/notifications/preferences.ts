
import { doc, getDoc, setDoc } from "firebase/firestore";
import { db } from "@/lib/firebase";

export async function getDailySelectionsPreference(
  uid: string
): Promise<boolean> {
  const reference = doc(
    db,
    "users",
    uid,
    "notificationPreferences",
    "defaults"
  );

  const snapshot = await getDoc(reference);

  return snapshot.exists()
    ? snapshot.data().dailySelectionsEnabled !== false
    : true;
}

export async function setDailySelectionsPreference(
  uid: string,
  enabled: boolean
): Promise<void> {
  const reference = doc(
    db,
    "users",
    uid,
    "notificationPreferences",
    "defaults"
  );

  await setDoc(
    reference,
    { dailySelectionsEnabled: enabled },
    { merge: true }
  );
}
