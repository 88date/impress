export type ServiceCallOptions = {
  /** Service version. Omit to use the highest known version. */
  version?: number;
};
export type ServiceAction = (
  parameters?: object,
  options?: ServiceCallOptions,
) => Promise<unknown>;
export type ServiceEventHandler = (payload?: object) => Promise<void> | void;
export type ServiceUnit = Record<string, ServiceAction> & {
  emit(eventName: string, payload?: object): Promise<void>;
  on(eventName: string, handler: ServiceEventHandler): void;
};
export type Services = Record<string, ServiceUnit>;
