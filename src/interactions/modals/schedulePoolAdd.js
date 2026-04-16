import { logger } from '../../utils/logger.js';
import { successEmbed, errorEmbed } from '../../utils/embeds.js';
import { handleInteractionError } from '../../utils/errorHandler.js';
import { getScheduleByName, appendPoolItem } from '../../services/scheduledMessagesService.js';

/**
 * Handles modal submission for /schedule pool-add.
 * customId format: schedule_pool_add:<name>
 * args: [name]
 */
async function execute(interaction, client, args) {
    try {
        const [name] = args;
        const guildId = interaction.guildId;
        const userId = interaction.user.id;
        const maxPoolItems = client?.config?.bot?.scheduler?.maxPoolItems ?? 500;

        const textContent = interaction.fields.getTextInputValue('text_content').trim() || null;
        const embedTitle = interaction.fields.getTextInputValue('embed_title').trim() || null;
        const embedDescription = interaction.fields.getTextInputValue('embed_description').trim() || null;
        const embedColorRaw = interaction.fields.getTextInputValue('embed_color').trim() || null;
        const weightRaw = interaction.fields.getTextInputValue('weight').trim() || null;

        if (!textContent && !embedTitle && !embedDescription) {
            await interaction.reply({
                embeds: [errorEmbed('No Content', 'Pool item needs at least some text or an embed title/description.')],
                ephemeral: true,
            });
            return;
        }

        const row = await getScheduleByName(client, guildId, name);
        if (!row) {
            await interaction.reply({
                embeds: [errorEmbed('Not Found', `Schedule \`${name}\` no longer exists.`)],
                ephemeral: true,
            });
            return;
        }

        const currentCount = Array.isArray(row.pool_payload) ? row.pool_payload.length : 0;
        if (currentCount >= maxPoolItems) {
            await interaction.reply({
                embeds: [errorEmbed('Pool Full', `Max pool size is ${maxPoolItems} items.`)],
                ephemeral: true,
            });
            return;
        }

        // Build the pool item object
        const item = {};
        if (textContent) item.text_content = textContent;

        const hasEmbed = embedTitle || embedDescription;
        if (hasEmbed) {
            item.embed_payload = {};
            if (embedTitle) item.embed_payload.title = embedTitle;
            if (embedDescription) item.embed_payload.description = embedDescription;
            if (embedColorRaw) {
                const hex = embedColorRaw.startsWith('#') ? embedColorRaw.slice(1) : embedColorRaw;
                const parsed = parseInt(hex, 16);
                if (!isNaN(parsed)) item.embed_payload.color = parsed;
            }
        }

        const weight = weightRaw ? parseInt(weightRaw, 10) : 1;
        if (!isNaN(weight) && weight > 0) item.weight = weight;

        const newPool = await appendPoolItem(client, guildId, name, item, userId);

        if (!newPool) {
            await interaction.reply({
                embeds: [errorEmbed('Error', 'Failed to add pool item. Try again.')],
                ephemeral: true,
            });
            return;
        }

        logger.info('schedule: pool item added', { name, guildId, userId, newCount: newPool.length });

        await interaction.reply({
            embeds: [successEmbed(
                'Pool Item Added',
                `Item added. Pool now has **${newPool.length}** item(s).\n` +
                `Index of new item: \`${newPool.length - 1}\``
            )],
            ephemeral: true,
        });
    } catch (error) {
        logger.error('schedule_pool_add: handler error', { error: error.message });
        await handleInteractionError(interaction, error, {
            commandName: 'schedule',
            source: 'schedule_pool_add_modal',
        });
    }
}

export default {
    name: 'schedule_pool_add',
    execute,
};
