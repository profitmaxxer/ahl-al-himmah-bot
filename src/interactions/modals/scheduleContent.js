import { logger } from '../../utils/logger.js';
import { successEmbed, errorEmbed } from '../../utils/embeds.js';
import { handleInteractionError } from '../../utils/errorHandler.js';
import { getScheduleByName, upsertSchedule } from '../../services/scheduledMessagesService.js';

/**
 * Handles modal submissions for /schedule create and /schedule edit.
 * customId format: schedule_content_modal:<name>:<mode>
 * args: [name, mode]
 */
async function execute(interaction, client, args) {
    try {
        const [name, mode] = args;
        const guildId = interaction.guildId;
        const userId = interaction.user.id;

        const textContent = interaction.fields.getTextInputValue('text_content').trim() || null;
        const embedTitle = interaction.fields.getTextInputValue('embed_title').trim() || null;
        const embedDescription = interaction.fields.getTextInputValue('embed_description').trim() || null;
        const embedColorRaw = interaction.fields.getTextInputValue('embed_color').trim() || null;

        // Validate that there's something to send
        const hasEmbed = embedTitle || embedDescription;
        if (!textContent && !hasEmbed) {
            await interaction.reply({
                embeds: [errorEmbed('No Content', 'You need at least some text or an embed title/description.')],
                ephemeral: true,
            });
            return;
        }

        // Build embed payload if any embed fields are set
        let embedPayload = null;
        if (hasEmbed) {
            embedPayload = {};
            if (embedTitle) embedPayload.title = embedTitle;
            if (embedDescription) embedPayload.description = embedDescription;
            if (embedColorRaw) {
                const hex = embedColorRaw.startsWith('#') ? embedColorRaw.slice(1) : embedColorRaw;
                const parsed = parseInt(hex, 16);
                if (!isNaN(parsed)) embedPayload.color = parsed;
            }
        }

        // Fetch existing row to preserve all other fields
        const existing = await getScheduleByName(client, guildId, name);
        if (!existing) {
            await interaction.reply({
                embeds: [errorEmbed('Not Found', `Schedule \`${name}\` no longer exists.`)],
                ephemeral: true,
            });
            return;
        }

        await upsertSchedule(client, {
            ...existing,
            text_content: textContent,
            embed_payload: embedPayload,
            updated_by: userId,
        });

        logger.info('schedule: content updated via modal', { name, guildId, userId });

        await interaction.reply({
            embeds: [successEmbed(
                'Content Saved',
                `Schedule \`${name}\` content updated.\n` +
                (existing.enabled
                    ? 'It is already enabled and will fire on schedule.'
                    : `Use \`/schedule enable name:${name}\` to activate it.`)
            )],
            ephemeral: true,
        });
    } catch (error) {
        logger.error('schedule_content_modal: handler error', { error: error.message });
        await handleInteractionError(interaction, error, {
            commandName: 'schedule',
            source: 'schedule_content_modal',
        });
    }
}

export default {
    name: 'schedule_content_modal',
    execute,
};
