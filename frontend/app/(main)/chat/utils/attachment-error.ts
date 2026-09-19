/**
 * One sentence for a chat attachment that failed to upload. The server's
 * reason often names the file already ("Couldn't read report.docx. …"), so it
 * is shown as-is then; otherwise the file is named once in front of it.
 */
export function attachmentErrorMessage(fileName: string, error: unknown): string {
  const reason =
    error instanceof Error || (error && typeof error === 'object' && 'message' in error)
      ? String((error as { message?: unknown }).message ?? '').trim()
      : '';
  if (reason && reason.includes(fileName)) return reason;
  return reason
    ? `Couldn't attach ${fileName}. ${reason}`
    : `Couldn't attach ${fileName}. Please try again.`;
}
