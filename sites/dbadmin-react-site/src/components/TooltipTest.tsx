import React, { useState } from 'react';
import * as Tooltip from '@radix-ui/react-tooltip';

export default function TooltipTest() {
    const [copied, setCopied] = useState(false);

    const handleCopy = () => {
        navigator.clipboard.writeText("Hello World").then(() => {
            setCopied(true);
            setTimeout(() => setCopied(false), 1000);
        });
    };

    return (
        <Tooltip.Provider>
            <Tooltip.Root delayDuration={200} open={copied || undefined}>
                <Tooltip.Trigger asChild>
                    <div
                        className="p-2 bg-gray-200 cursor-pointer"
                        onClick={handleCopy}
                    >
                        Hover & click to copy
                    </div>
                </Tooltip.Trigger>
                <Tooltip.Portal>
                    <Tooltip.Content
                        className={`rounded px-2 py-1 text-xs text-white ${copied ? 'bg-orange-600' : 'bg-black'
                            }`}
                        side="top"
                        sideOffset={5}
                    >
                        {copied ? 'Copied!' : 'Click to copy'}
                        <Tooltip.Arrow className={copied ? 'fill-orange-600' : 'fill-black'} />
                    </Tooltip.Content>
                </Tooltip.Portal>
            </Tooltip.Root>
        </Tooltip.Provider>
    );
}
