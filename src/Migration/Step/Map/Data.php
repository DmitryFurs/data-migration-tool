<?php
/**
 * Copyright © Magento, Inc. All rights reserved.
 * See COPYING.txt for license details.
 */
namespace Migration\Step\Map;

use Migration\App\Progress;
use Migration\App\ProgressBar\LogLevelProcessor;
use Migration\App\Step\StageInterface;
use Migration\Logger\Logger;
use Migration\Logger\Manager as LogManager;
use Migration\Reader\Map;
use Migration\Reader\MapFactory;
use Migration\Reader\MapInterface;
use Migration\RecordTransformer;
use Migration\RecordTransformerFactory;
use Migration\ResourceModel\DirectCopy;
use Migration\ResourceModel\Document;
use Migration\ResourceModel\Record;
use Migration\ResourceModel\Destination;
use Migration\ResourceModel\RecordFactory;
use Migration\ResourceModel\Source;

/**
 * Class Data
 * @SuppressWarnings(PHPMD.CouplingBetweenObjects)
 * @SuppressWarnings(CyclomaticComplexity)
 */
class Data implements StageInterface
{
    /**
     * @var Source
     */
    private $source;

    /**
     * @var Destination
     */
    private $destination;

    /**
     * @var RecordFactory
     */
    private $recordFactory;

    /**
     * @var Map
     */
    private $map;

    /**
     * @var RecordTransformerFactory
     */
    private $recordTransformerFactory;

    /**
     * @var LogLevelProcessor
     */
    private $progressBar;

    /**
     * Progress instance, saves the state of the process
     *
     * @var Progress
     */
    private $progress;

    /**
     * @var Logger
     */
    private $logger;

    /**
     * @var Helper
     */
    private $helper;

    /**
     * @var DirectCopy
     */
    private $directCopy;

    /**
     * @param LogLevelProcessor $progressBar
     * @param Source $source
     * @param Destination $destination
     * @param RecordFactory $recordFactory
     * @param RecordTransformerFactory $recordTransformerFactory
     * @param MapFactory $mapFactory
     * @param Progress $progress
     * @param Logger $logger
     * @param Helper $helper
     * @param DirectCopy $directCopy
     *
     * @SuppressWarnings(CyclomaticComplexity)
     * @SuppressWarnings(PHPMD.ExcessiveParameterList)
     */
    public function __construct(
        LogLevelProcessor $progressBar,
        Source $source,
        Destination $destination,
        RecordFactory $recordFactory,
        RecordTransformerFactory $recordTransformerFactory,
        MapFactory $mapFactory,
        Progress $progress,
        Logger $logger,
        Helper $helper,
        DirectCopy $directCopy
    ) {
        $this->source = $source;
        $this->destination = $destination;
        $this->recordFactory = $recordFactory;
        $this->recordTransformerFactory = $recordTransformerFactory;
        $this->map = $mapFactory->create('map_file');
        $this->progressBar = $progressBar;
        $this->progress = $progress;
        $this->logger = $logger;
        $this->helper = $helper;
        $this->directCopy = $directCopy;
    }

    /**
     * @inheritdoc
     */
    public function perform()
    {
        $this->progressBar->start(count($this->source->getDocumentList()), LogManager::LOG_LEVEL_INFO);
        $sourceDocuments = $this->source->getDocumentList();
        $stage = 'run';
        $processedDocuments = $this->progress->getProcessedEntities($this, $stage);
        foreach (array_diff($sourceDocuments, $processedDocuments) as $sourceDocName) {
            $this->progressBar->advance(LogManager::LOG_LEVEL_INFO);
            $sourceDocument = $this->source->getDocument($sourceDocName);
            $destinationName = $this->map->getDocumentMap($sourceDocName, MapInterface::TYPE_SOURCE);
            if (!$destinationName) {
                continue;
            }
            $destDocument = $this->destination->getDocument($destinationName);
            if (!$destDocument) {
                continue;
            }
            $this->destination->clearDocument($destinationName);
            $this->logger->debug('migrating', ['table' => $sourceDocName]);

            $isCopiedDirectly = $this->directCopy->execute($sourceDocument, $destDocument, $this->map);
            if ($isCopiedDirectly) {
                $this->progressBar->start(1, LogManager::LOG_LEVEL_DEBUG);
            } else {
                $pageNumber = 0;
                $this->progressBar->start(
                    ceil($this->source->getRecordsCount($sourceDocName) / $this->source->getPageSize($sourceDocName)),
                    LogManager::LOG_LEVEL_DEBUG
                );

                $recordTransformer = $this->getRecordTransformer($sourceDocument, $destDocument);
                while (!empty($items = $this->source->getRecords($sourceDocName, $pageNumber))) {
                    $pageNumber++;
                    $destinationRecords = $destDocument->getRecords();
                    foreach ($items as $data) {
                        if ($recordTransformer) {
                            /** @var Record $record */
                            $record = $this->recordFactory->create(['document' => $sourceDocument, 'data' => $data]);
                            /** @var Record $destRecord */
                            $destRecord = $this->recordFactory->create(['document' => $destDocument]);
                            $recordTransformer->transform($record, $destRecord);
                        } else {
                            $destRecord = $this->recordFactory->create(['document' => $destDocument, 'data' => $data]);
                        }
                        $destinationRecords->addRecord($destRecord);
                    }
                    $this->source->setLastLoadedRecord($sourceDocName, end($items));
                    $this->progressBar->advance(LogManager::LOG_LEVEL_DEBUG);
                    $fieldsUpdateOnDuplicate = $this->helper->getFieldsUpdateOnDuplicate($destinationName);
                    $this->destination->saveRecords($destinationName, $destinationRecords, $fieldsUpdateOnDuplicate);
                }
            }
            $this->source->setLastLoadedRecord($sourceDocName, []);
            $this->progress->addProcessedEntity($this, $stage, $sourceDocName);
            $this->progressBar->finish(LogManager::LOG_LEVEL_DEBUG);
        }
        $this->progressBar->finish(LogManager::LOG_LEVEL_INFO);
        return true;
    }

    /**
     * Get record transformer
     *
     * @param Document $sourceDocument
     * @param Document $destDocument
     * @return RecordTransformer
     */
    public function getRecordTransformer(Document $sourceDocument, Document $destDocument)
    {
        /** @var RecordTransformer $recordTransformer */
        $recordTransformer = $this->recordTransformerFactory->create(
            [
                'sourceDocument' => $sourceDocument,
                'destDocument' => $destDocument,
                'mapReader' => $this->map,
            ]
        );

        return $recordTransformer->init();
    }
}
