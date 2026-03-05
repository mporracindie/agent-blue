import { ChannelAdapter } from "../../core/interfaces.js";

export class ConsoleChannelAdapter implements ChannelAdapter {
  async send(text: string): Promise<void> {
    process.stdout.write(`${text}\n`);
  }
}
