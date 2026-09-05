export type ServiceCallOptions = {
  /** Service version. Omit to use the highest known version. */
  version?: number;
};
export type ServiceAction = (
  parameters?: object,
  options?: ServiceCallOptions,
) => Promise<unknown>;
export type ServiceUnit = Record<string, ServiceAction>;
export type Services = Record<string, ServiceUnit>;
